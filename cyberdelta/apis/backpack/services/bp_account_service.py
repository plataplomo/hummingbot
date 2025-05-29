"""
CyberDeltaEngine: Backpack Account Service
------------------------------------------

This service encapsulates the logic for fetching and managing account-specific
information from the Backpack Exchange. It uses the HttpClient, BackpackRequestBuilder,
BackpackResponseHandler, and BackpackAccountDataMapper to interact with the API
and returns Internal Domain Models.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING  # Keep Any for **kwargs in withdraw

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler, RawJsonResponse
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,  # Reverted to MarginAccountSummary
    Order,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.operations import Transfer, Withdrawal

if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


class BackpackAccountService:
    """
    Service class for Backpack account management operations.
    Returns Internal Domain Models.
    """

    _http_client_requester: HttpClientRequesterSig
    _request_builder: BackpackRequestBuilder
    _response_handler: BackpackResponseHandler
    _mapper: BackpackAccountDataMapper
    _authenticator: IAuthenticator | None
    _exchange_name: str
    _rate_limiter_service: RateLimiterService

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        rate_limiter_service: RateLimiterService,
        mapper: BackpackAccountDataMapper | None = None,
    ) -> None:
        """
        Initialize the BackpackAccountService.

        Args:
            http_client_requester: A callable for making API requests.
            request_builder: An instance of BackpackRequestBuilder.
            response_handler: An instance of BackpackResponseHandler.
            authenticator: An instance of IAuthenticator for signed requests.
            exchange_name: The name of the exchange.
            rate_limiter_service: Service for managing API rate limits.
            mapper: Optional mapper instance for dependency injection.
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._mapper = (
            mapper or BackpackAccountDataMapper()
        )  # Updated to use account-specific mapper
        self._rate_limiter_service = rate_limiter_service

    async def _get_raw_balances_dict(self) -> dict[str, BackpackRawBalance]:
        """Helper to fetch and validate raw account balances dictionary."""
        endpoint_path = "/api/v1/capital"
        params = self._request_builder.build_get_balances_params()  # Returns None
        logger.debug(
            f"[{self._exchange_name}] Requesting raw balances dict from {endpoint_path} "
            f"with params: {params}"
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        # headers: Mapping[str, str] = {}
        # Not strictly needed if not used beyond _http_client_requester
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw balances dict response: {raw_data!r} "
                f"(Status: {status_code})"
            )
            if raw_data is None:
                raise APIError(
                    message=(f"No data received for raw balances dict, status: {status_code}"),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            return self._response_handler.handle_get_balances_response(raw_data)
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for raw balances dict: {e_val}. "
                f"Raw: {raw_data!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing raw balances dict data failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for raw balances dict: {e_unhandled}. "
                f"{raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for raw balances dict: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def _get_raw_positions_list(self, symbol: str | None = None) -> list[BackpackRawPosition]:
        """Helper to fetch and validate raw current open positions list."""
        endpoint_path = "/api/v1/positions"
        # build_get_positions_params returns None if symbol is None, or {"symbol": symbol}
        params = self._request_builder.build_get_positions_params(symbol)
        logger.debug(
            f"[{self._exchange_name}] Requesting raw positions from {endpoint_path} "
            f"for symbol '{symbol or 'all'}' with params: {params}"
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw positions response for '{symbol or 'all'}: "
                f"{raw_data!r} (Status: {status_code})"
            )
            if raw_data is None:
                # Consider if an empty list is a valid response for no positions vs. an error
                # For now, if raw_data is None, it implies an issue not caught by _request
                # or an unexpected 204 No Content for an endpoint that should return data.
                raise APIError(
                    message=(
                        f"No data received for raw positions for '{symbol or 'all'}'"
                        f", status: {status_code}"
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            # The response_handler.handle_get_positions_response now correctly handles
            # dict for single symbol or list for all symbols.
            return self._response_handler.handle_get_positions_response(raw_data, symbol)
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for raw positions ('{symbol or 'all'}'): {e_val}. "
                f"Raw: {raw_data!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing raw positions data failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for raw positions ('{symbol or 'all'}'): {e_unhandled}. "
                f"{raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for raw positions: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def _get_raw_account_summary_obj(self) -> BackpackRawAccountSummary:
        """Helper to fetch and validate the raw account summary object."""
        endpoint_path = "/api/v1/account"
        params = self._request_builder.build_get_account_info_params()  # Returns None
        logger.debug(
            f"[{self._exchange_name}] Requesting raw account summary from {endpoint_path} "
            f"with params: {params}"
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw account summary response: {raw_data!r} "
                f"(Status: {status_code})"
            )
            if raw_data is None:
                raise APIError(
                    message=(f"No data received for raw account summary, status: {status_code}"),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            return self._response_handler.handle_get_account_info_response(raw_data)
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for raw account summary: {e_val}. "
                f"Raw: {raw_data!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing raw account summary data failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for raw account summary: {e_unhandled}. "
                f"{raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for raw account summary: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Retrieves all spot balances from the account."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_balances"

        # No input parameters to validate for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.info(f"[{self._exchange_name}] Getting account balances.")
            raw_balances_payload = await self._get_raw_balances_dict()
            internal_balances: dict[str, SpotBalance] = {}
            # raw_balances_payload is dict[str, BackpackRawBalance],
            # so raw_balance_model is BackpackRawBalance
            for asset_symbol, raw_balance_model in raw_balances_payload.items():
                try:
                    # Ensure asset_symbol is str, as dict keys from JSON can be various types
                    # if not strictly validated by Pydantic at an earlier stage.
                    # However, raw_balances_payload keys are from response_handler,
                    # which should ensure str keys.
                    internal_balances[asset_symbol] = (
                        self._mapper.transform_raw_balance_to_internal(
                            asset_symbol, raw_balance_model
                        )
                    )
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        f"[{self._exchange_name}] Skipping balance mapping for asset "
                        f"'{asset_symbol}' due to error: {e_map_item}. Raw: {raw_balance_model!r}"
                    )
            logger.debug(
                f"[{self._exchange_name}] Successfully mapped {len(internal_balances)} "
                f"spot balances."
            )
            return internal_balances

        except APIError:
            # Re-raise APIErrors from _get_raw_balances_dict, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error: "
                f"{e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure: "
                f"{e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Retrieves open derivative positions, optionally filtered by symbol."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_positions"

        if symbol is not None and not symbol:
            raise ValueError(
                f"[{current_method}] 'symbol' must be a non-empty string when provided."
            )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.info(
                f"[{self._exchange_name}] Getting derivative positions for symbol "
                f"'{symbol or 'all'}'."
            )
            raw_positions_list = await self._get_raw_positions_list(symbol)
            internal_positions: list[DerivativePosition] = []
            for raw_position_model in raw_positions_list:
                try:
                    internal_positions.append(
                        self._mapper.transform_raw_position_to_internal(raw_position_model)
                    )
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        f"[{self._exchange_name}] Skipping position mapping for raw position "
                        f"'{
                            (
                                raw_position_model.symbol
                                if hasattr(raw_position_model, 'symbol')
                                else 'UnknownSymbol'
                            )
                        }' "
                        f"due to error: {e_map_item}. Raw: {raw_position_model!r}"
                    )
            logger.debug(
                f"[{self._exchange_name}] Successfully mapped {len(internal_positions)} "
                f"derivative positions."
            )
            return internal_positions

        except APIError:
            # Re-raise APIErrors from _get_raw_positions_list, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol or 'all'}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol or 'all'}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            # Check if this is from our own input parameter validation
            # Input parameter validation errors should propagate as ValueError
            # Service logic errors should be wrapped as APIError
            error_msg = str(e_service_logic)
            if current_method in error_msg and "symbol" in error_msg:
                # This is likely from our input parameter validation - re-raise as is
                raise
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for {symbol or 'all'}: {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {symbol or 'all'}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_account_info(self) -> MarginAccountSummary:
        """Retrieves general account information or summary."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_account_info"

        # No input parameters to validate for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.debug(
                f"[{self._exchange_name}] Fetching account info (summary, balances, positions)."
            )
            # Fetch raw components concurrently if desired, or sequentially.
            # For simplicity and clarity, fetching sequentially here.
            # Concurrency can be added with asyncio.gather if performance becomes an issue.
            raw_settings = await self._get_raw_account_summary_obj()
            raw_balances_dict = await self._get_raw_balances_dict()
            raw_positions_list = await self._get_raw_positions_list()  # Will be [] for Backpack

            # Now pass all components to the mapper
            internal_summary = self._mapper.transform_raw_account_summary_to_internal(
                raw_settings=raw_settings,
                spot_balances_raw=raw_balances_dict,
                derivative_positions_raw=raw_positions_list,  # Expects list[BackpackRawPosition]
            )
            logger.debug(
                f"[{self._exchange_name}] Mapped internal account summary: {internal_summary}"
            )
            return internal_summary

        except APIError:
            # Re-raise APIErrors from _get_raw_* methods, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error: "
                f"{e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure: "
                f"{e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def transfer(
        self,
        asset: str,
        amount: Decimal,
        from_account_type: str,
        to_account_type: str,
        client_transfer_id: str | None = None,
    ) -> Transfer:
        """Performs an internal transfer of funds between account types."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "transfer"

        if not asset:
            raise ValueError(f"[{current_method}] 'asset' must be a non-empty string.")
        if not amount.is_finite() or amount <= 0:
            raise ValueError(f"[{current_method}] 'amount' must be a positive finite Decimal.")
        if not from_account_type:
            raise ValueError(f"[{current_method}] 'from_account_type' must be a non-empty string.")
        if not to_account_type:
            raise ValueError(f"[{current_method}] 'to_account_type' must be a non-empty string.")

        # Additional validation for whitespace-only strings
        if not asset.strip():
            raise ValueError(f"[{current_method}] 'asset' cannot be empty or whitespace only.")
        if not from_account_type.strip():
            raise ValueError(
                f"[{current_method}] 'from_account_type' cannot be empty or whitespace only."
            )
        if not to_account_type.strip():
            raise ValueError(
                f"[{current_method}] 'to_account_type' cannot be empty or whitespace only."
            )

        # Business Logic Pre-Validation (moved from RequestBuilder)
        valid_accounts = {"SPOT", "MARGIN", "FUTURES"}
        if from_account_type not in valid_accounts:
            raise ValueError(
                f"[{current_method}] Invalid from_account_type: {from_account_type}. "
                f"Must be one of {valid_accounts}"
            )
        if to_account_type not in valid_accounts:
            raise ValueError(
                f"[{current_method}] Invalid to_account_type: {to_account_type}. "
                f"Must be one of {valid_accounts}"
            )
        if from_account_type == to_account_type:
            raise ValueError(
                f"[{current_method}] from_account_type and to_account_type cannot be the same"
            )

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint_path = "/api/v1/capital/transfer"
            payload = self._request_builder.build_internal_transfer_payload(
                asset_symbol=asset,
                amount=amount,
                from_account=from_account_type,
                to_account=to_account_type,
                client_transfer_id=client_transfer_id,
            )
            logger.debug(
                f"[{self._exchange_name}] Requesting transfer from {endpoint_path} "
                f"with payload: {payload}"
            )

            raw_data, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=payload,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                f"[{self._exchange_name}] Raw transfer response: {raw_data!r} "
                f"(Status: {status_code})"
            )
            if raw_data is None:
                raise APIError(
                    message=f"No data received for transfer, status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            # Assuming self._response_handler.handle_transfer_response exists
            # and validates raw_data, returning it as RawJsonResponse
            # (typically a dict for transfers).
            validated_raw_json_response: RawJsonResponse = (
                self._response_handler.handle_transfer_response(raw_data)
            )
            # The mapper's transform_raw_transfer_to_internal expects RawJsonResponse.
            # We need to ensure validated_raw_json_response is a dict as per
            # mapper's likely expectation for transfer data.
            if not isinstance(validated_raw_json_response, dict):
                raise APIError(
                    message=(
                        f"Transfer response handler returned unexpected type: "
                        f"{type(validated_raw_json_response)}, expected dict."
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            internal_transfer = self._mapper.transform_raw_transfer_to_internal(
                raw_response=validated_raw_json_response,  # Pass validated dict
                exchange_name=self._exchange_name,  # Pass exchange_name
                asset=asset,
                quantity=amount,
                from_account_type_raw=from_account_type,
                to_account_type_raw=to_account_type,
                client_transfer_id=client_transfer_id,
            )
            logger.debug(f"[{self._exchange_name}] Mapped internal transfer: {internal_transfer}")
            logger.debug(
                f"[{self._exchange_name}] Transfer successful. "
                f"Response: {internal_transfer.model_dump_json(exclude_none=True)}"
            )
            return internal_transfer

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for transfer: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for transfer: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            # Check if this is from our own input parameter validation
            # Input parameter validation errors should propagate as ValueError
            # Service logic errors should be wrapped as APIError
            error_msg = str(e_service_logic)
            if current_method in error_msg and any(
                param in error_msg
                for param in ["asset", "amount", "from_account_type", "to_account_type"]
            ):
                # This is likely from our input parameter validation - re-raise as is
                raise
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for transfer: {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for transfer: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def withdraw(
        self,
        asset: str,
        amount: Decimal,
        address: str,
        network: str | None = None,
        tag: str | None = None,
        client_withdrawal_id: str | None = None,
        two_factor_token: str | None = None,
        **_kwargs: dict[str, str],  # For potential extra params not yet defined
    ) -> Withdrawal:
        """Initiates a withdrawal of funds to an external address."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "withdraw"

        if not asset:
            raise ValueError(f"[{current_method}] 'asset' must be a non-empty string.")
        if not amount.is_finite() or amount <= 0:
            raise ValueError(f"[{current_method}] 'amount' must be a positive finite Decimal.")
        if not address:
            raise ValueError(f"[{current_method}] 'address' must be a non-empty string.")

        # Business Logic Pre-Validation (moved from RequestBuilder)
        if network is None:
            raise ValueError(f"[{current_method}] 'network' is required for withdrawal.")

        # Validate asset is not empty after stripping whitespace
        if not asset.strip():
            raise ValueError(f"[{current_method}] 'asset' cannot be empty or whitespace only.")

        # Validate address is not empty after stripping whitespace
        if not address.strip():
            raise ValueError(f"[{current_method}] 'address' cannot be empty or whitespace only.")

        # Validate network maps to supported blockchain
        blockchain_mapping = {
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

        if network not in blockchain_mapping:
            raise ValueError(
                f"[{current_method}] Unsupported network: {network}. "
                f"Supported networks: {list(blockchain_mapping.keys())}"
            )

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint_path = "/api/v1/capital/withdrawals"
            payload = self._request_builder.build_withdraw_payload(
                asset=asset,
                amount=amount,
                address=address,
                network=network,
                tag=tag,
                client_withdrawal_id=client_withdrawal_id,
                two_factor_token=two_factor_token,
                # **kwargs are not explicitly passed to builder unless it accepts them.
                # Assuming builder only takes defined params for now.
            )
            logger.debug(
                f"[{self._exchange_name}] Requesting withdrawal from {endpoint_path} "
                f"with payload: {payload}"
            )

            raw_data, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=payload,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                f"[{self._exchange_name}] Raw withdrawal response: {raw_data!r} "
                f"(Status: {status_code})"
            )
            if raw_data is None:
                raise APIError(
                    message=f"No data received for withdrawal, status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # Assuming self._response_handler.handle_withdraw_response exists and
            # returns BackpackRawWithdrawalResponse
            raw_withdrawal_model: BackpackRawWithdrawalResponse = (
                self._response_handler.handle_withdraw_response(raw_data)
            )

            internal_withdrawal = self._mapper.transform_raw_withdrawal_response_to_internal(
                raw_response=raw_withdrawal_model,
                asset=asset,
                quantity=amount,
                address=address,
                network=network,
                client_withdrawal_id=client_withdrawal_id,
                tag=tag,
            )
            logger.debug(
                f"[{self._exchange_name}] Mapped internal withdrawal: {internal_withdrawal}"
            )
            return internal_withdrawal

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for withdrawal: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for withdrawal: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            # Check if this is from our own input parameter validation
            # Input parameter validation errors should propagate as ValueError
            # Service logic errors should be wrapped as APIError
            error_msg = str(e_service_logic)
            if current_method in error_msg and any(
                param in error_msg for param in ["asset", "amount", "address"]
            ):
                # This is likely from our input parameter validation - re-raise as is
                raise
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for withdrawal: {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for withdrawal: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_order_history(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = 100,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> list[Order]:
        """Retrieves historical order data."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_history"

        if limit is not None and limit <= 0:
            raise ValueError(f"[{current_method}] 'limit' must be positive if provided.")
        if start_time is not None and end_time is not None and start_time >= end_time:
            raise ValueError(f"[{current_method}] 'start_time' must be before 'end_time'.")

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint_path = "/api/v1/history/orders"
            start_time_ms = int(start_time.timestamp() * 1000) if start_time else None
            end_time_ms = int(end_time.timestamp() * 1000) if end_time else None

            params = self._request_builder.build_get_order_history_params(
                symbol=symbol,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                limit=limit,
                order_id=order_id,
                client_order_id=client_order_id,
            )
            logger.debug(
                f"[{self._exchange_name}] Requesting order history from {endpoint_path} "
                f"with params: {params}"
            )

            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                f"[{self._exchange_name}] Raw order history response: {raw_data!r} "
                f"(Status: {status_code})"
            )
            if raw_data is None:
                raise APIError(
                    message=f"No data received for order history, status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_orders_list: list[BackpackRawOrder] = (
                self._response_handler.handle_get_order_history_response(raw_data, symbol)
            )

            internal_orders: list[Order] = []
            for raw_order_model in raw_orders_list:
                try:
                    internal_orders.append(
                        self._mapper.transform_raw_order_to_internal(raw_order_model)
                    )
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        f"[{self._exchange_name}] Skipping order history mapping for "
                        f"order '{raw_order_model.clientId or raw_order_model.id}' "
                        f"due to error: {e_map_item}. Raw: "
                        f"{raw_order_model.model_dump_json(exclude_none=True)}"
                    )

            logger.debug(
                f"[{self._exchange_name}] Mapped internal order history: "
                f"{len(internal_orders)} orders"
            )
            return internal_orders

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for order history: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for order history: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            # Check if this is from our own input parameter validation
            # Input parameter validation errors should propagate as ValueError
            # Service logic errors should be wrapped as APIError
            error_msg = str(e_service_logic)
            if current_method in error_msg and any(
                param in error_msg for param in ["limit", "start_time", "end_time"]
            ):
                # This is likely from our input parameter validation - re-raise as is
                raise
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for order history: {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for order history: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_trade_history(
        self,
        symbol: str | None = None,
        limit: int | None = 100,  # Match service stub signature
    ) -> list[Trade]:
        """Retrieves historical trade data (fills)."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_trade_history"

        if limit is not None and limit <= 0:
            raise ValueError(f"[{current_method}] 'limit' must be positive if provided.")

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint_path = "/api/v1/history/fills"

            # The builder supports more parameters, pass None for those not in service signature
            params = self._request_builder.build_get_trade_history_params(
                symbol=symbol,
                limit=limit,
                start_time_ms=None,  # Not in service signature
                end_time_ms=None,  # Not in service signature
                from_id=None,  # Not in service signature
            )
            logger.debug(
                f"[{self._exchange_name}] Requesting trade history from {endpoint_path} "
                f"with params: {params}"
            )

            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                f"[{self._exchange_name}] Raw trade history response: {raw_data!r} "
                f"(Status: {status_code})"
            )
            if raw_data is None:
                raise APIError(
                    message=f"No data received for trade history, status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_trades_list: list[BackpackRawTrade] = (
                self._response_handler.handle_get_trade_history_response(raw_data, symbol)
            )

            internal_trades: list[Trade] = []
            for raw_trade_model in raw_trades_list:
                try:
                    trade = self._mapper.transform_raw_trade_to_internal(raw_trade_model)
                    if trade is not None:  # Mapper can return None
                        internal_trades.append(trade)
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        f"[{self._exchange_name}] Skipping trade history mapping for trade "
                        f"'{raw_trade_model.id}' due to error: {e_map_item}. Raw: "
                        f"{raw_trade_model.model_dump_json(exclude_none=True)}"
                    )

            logger.debug(
                f"[{self._exchange_name}] Mapped internal trade history: "
                f"{len(internal_trades)} trades"
            )
            return internal_trades

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for trade history: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for trade history: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            # Check if this is from our own input parameter validation
            # Input parameter validation errors should propagate as ValueError
            # Service logic errors should be wrapped as APIError
            error_msg = str(e_service_logic)
            if current_method in error_msg and "limit" in error_msg:
                # This is likely from our input parameter validation - re-raise as is
                raise
            else:
                # This is from service internal logic - wrap as APIError
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for trade history: {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for trade history: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected
