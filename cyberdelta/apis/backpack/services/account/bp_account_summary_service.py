"""Backpack Account Summary Service.

This service handles account summary and settings operations for the Backpack exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Account summary retrieval (basic and enhanced)
- Account settings updates
- Subaccount support
- Collateral-enhanced summary data
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping
from http import HTTPStatus
from typing import TYPE_CHECKING, NoReturn

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackAccountSummaryMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummaryResponse
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionResponse
from cyberdelta.apis.backpack.protocols.builder_protocols import AccountRequestBuilderProtocol
from cyberdelta.apis.backpack.protocols.handler_protocols import AccountResponseHandlerProtocol
from cyberdelta.apis.backpack.protocols.mapper_protocols import AccountSummaryMapperProtocol
from cyberdelta.apis.backpack.services.account.bp_account_state_service import (
    BackpackAccountStateService,
)
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.base.trading_execution_domain import (
    AccountSettings as DomainAccountSettings,
    AccountSettingsPolicy,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions import EmptyResponseError
from cyberdelta.apis.exceptions.response_validation import UnreachableCodeError
from cyberdelta.apis.models.service_args.account import UpdateAccountSettingsArgs
from cyberdelta.apis.utils import ensure_dict_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import AccountSettings, MarginAccountSummary
from cyberdelta.core.symbols import exchanges
from cyberdelta.utils.typing import ParsedJsonResponse, is_dict_response, is_list_response


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]

# Validation constants
UINT16_MAX_VALUE = 65535  # Maximum value for 16-bit unsigned integer


class BackpackAccountSummaryService:
    """Focused service for Backpack account summary and settings operations.

    Handles retrieval of account overview data, settings management,
    and enhanced summary with collateral integration.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: AccountRequestBuilderProtocol,
        response_handler: AccountResponseHandlerProtocol,
        authenticator: IAuthenticator | None,
        exchange_name: str = "backpack",
        # Optional dependency injection for shared state service (Hyperliquid pattern)
        account_state_service: BackpackAccountStateService | None = None,
        # Optional dependency injection for mapper
        mapper: AccountSummaryMapperProtocol | None = None,
    ) -> None:
        """Initialize the account summary service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            account_state_service: Optional shared account state service
                (creates default if not provided)
            mapper: Optional account summary mapper instance for dependency injection
                (creates default if not provided)
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name

        # Shared state service injection (following Hyperliquid clearinghouse pattern)
        self._account_state_service = account_state_service or BackpackAccountStateService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
        )

        # Provide sensible default if mapper not injected
        self._mapper = mapper or BackpackAccountSummaryMapper()

    async def get_account_summary(self, subaccount_id: int | None = None) -> MarginAccountSummary:
        """Get comprehensive account margin summary.

        Attempts to use enhanced collateral endpoint first for richer data,
        falls back to basic implementation if unavailable.

        Args:
            subaccount_id: Optional subaccount ID (OpenAPI uint16, 0-65535)

        Returns:
            MarginAccountSummary with bp_details extension slot populated

        Raises:
            APIError: If account data cannot be retrieved
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_account_summary"

        # Validate subaccount_id if provided (OpenAPI spec: uint16)
        if subaccount_id is not None and (subaccount_id < 0 or subaccount_id > UINT16_MAX_VALUE):
            raise APIError(
                code=APIErrorCode.INVALID_REQUEST.value,
                message=f"Invalid subaccount_id: {subaccount_id}. Must be uint16 (0-65535).",
                http_status=400,
            )

        logger.debug(
            "getting_account_summary",
            exchange=self._exchange_name,
            method=current_method,
            subaccount_id=subaccount_id,
            message="Getting account summary",
        )

        try:
            # Always try to use enhanced transformation with collateral data first
            # This provides the most comprehensive account information
            raw_settings = await self._get_raw_account_summary_obj()
            raw_positions_list = await self._get_raw_positions_list()

            try:
                # Use shared state service for collateral data (following Hyperliquid pattern)
                raw_collateral = await self._account_state_service.get_account_state(subaccount_id)
                # Use enhanced mapper method that includes collateral data with IMF/MMF
                return self._mapper.transform_enhanced_account_data_to_margin_summary(
                    raw_collateral=raw_collateral,
                    raw_settings=raw_settings,
                    raw_positions=raw_positions_list,
                )
            except (APIError, ValidationError, TransformationError):
                # If shared state service fails, fall back to basic transformation
                logger.debug(
                    "shared_state_service_failed_using_basic_transformation",
                    exchange=self._exchange_name,
                    subaccount_id=subaccount_id,
                    message="Shared state service failed, falling back to basic transformation",
                )

            # Fall back to basic transformation using balances
            raw_balances_dict = await self._get_raw_balances_dict()

            # Transform using basic mapper method
            return self._mapper.transform_raw_account_summary_to_internal(
                raw_settings=raw_settings,
                spot_balances_raw=raw_balances_dict,
                derivative_positions_raw=raw_positions_list,
            )

        except APIError:
            raise
        except (ValueError, TypeError) as e:
            logger.exception(
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Service logic error",
            )
            raise APIError(
                code=APIErrorCode.INVALID_REQUEST.value,
                message="Invalid request parameters.",
                original_exception=e,
                http_status=400,
            ) from e
        except Exception as e:
            logger.exception(
                "unexpected_service_failure",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Unexpected service failure",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e,
            ) from e

    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Update account settings such as leverage limits and auto-trading preferences.

        Args:
            args: Account settings to update

        Returns:
            AccountSettings: Updated account settings with current timestamp

        Raises:
            APIError: If the request fails or settings cannot be updated
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "update_account_settings"

        logger.debug(
            "updating_account_settings",
            exchange=self._exchange_name,
            method=current_method,
            message="Starting account settings update",
        )

        try:
            # Build payload using request builder
            # Convert Decimal leverage to int for API
            leverage_int = int(args.leverage_limit) if args.leverage_limit is not None else None

            # Convert boolean auto_lend to AccountSettings domain object
            automation_policy = (
                AccountSettingsPolicy.AUTO_LENDING
                if args.auto_lend
                else AccountSettingsPolicy.MANUAL_CONTROL
            )

            # Create AccountSettings with appropriate leverage handling
            if leverage_int is not None:
                account_settings = DomainAccountSettings(
                    leverage_limit=leverage_int,
                    automation_policy=automation_policy,
                )
            else:
                # If leverage not specified, use domain default (10) - user only updating automation
                account_settings = DomainAccountSettings(
                    automation_policy=automation_policy,
                    # leverage_limit will use Field default of 10
                )

            payload = self._request_builder.build_update_account_settings_payload(
                leverage=leverage_int,
                account_settings=account_settings,
            )

            endpoint_path = "/api/v1/account"
            logger.debug(
                "executing_settings_update",
                exchange=self._exchange_name,
                endpoint_path=endpoint_path,
                payload=payload,
                message="Executing account settings update",
            )

            # Execute PATCH request
            _, status_code, _ = await self._http_client_requester(
                method="PATCH",
                endpoint=endpoint_path,
                data=payload,
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.SIGNED,
                    endpoint_group="private",
                    request_weight=1,
                ),
            )

            logger.debug(
                "settings_update_successful",
                exchange=self._exchange_name,
                method=current_method,
                status_code=status_code,
                message="Account settings updated successfully",
            )

            # Transform updated settings to internal model
            return self._mapper.transform_account_settings_update_to_internal(
                args,
                self._exchange_name,
            )

        except APIError:
            raise
        except Exception as e:
            logger.exception(
                "settings_update_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Failed to update account settings",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in update_account_settings",
                original_exception=e,
            ) from e

    async def _get_enhanced_account_info(
        self,
        subaccount_id: int | None = None,
    ) -> MarginAccountSummary | None:
        """Enhanced account info using collateral endpoint.

        Fetches comprehensive margin data from /api/v1/capital/collateral
        with parallel fetching of positions data for complete account state.

        Args:
            subaccount_id: Optional subaccount identifier

        Returns:
            MarginAccountSummary or None if collateral endpoint unavailable

        Raises:
            APIError: If data transformation fails or other API errors occur
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "_get_enhanced_account_info"

        try:
            logger.debug(
                "fetching_enhanced_account_data",
                exchange=self._exchange_name,
                method=current_method,
                with_collateral=True,
                message="Fetching enhanced account data with collateral",
            )

            # Fetch collateral, settings, and positions data in parallel
            # Use shared state service for collateral data (following Hyperliquid pattern)
            collateral_task = self._account_state_service.get_account_state(subaccount_id)
            settings_task = self._get_raw_account_summary_obj()
            positions_task = self._get_raw_positions_list()

            raw_collateral, raw_settings, raw_positions_list = await asyncio.gather(
                collateral_task,
                settings_task,
                positions_task,
                return_exceptions=False,
            )

            # Transform using enhanced mapper method
            internal_summary = self._mapper.transform_enhanced_account_data_to_margin_summary(
                raw_collateral=raw_collateral,
                raw_settings=raw_settings,
                raw_positions=raw_positions_list,
            )

            logger.debug(
                "enhanced_account_summary_created",
                exchange=self._exchange_name,
                method=current_method,
                message="Enhanced account summary created successfully",
            )

        except APIError as e:
            # If collateral endpoint not available (404) or returns invalid data,
            # return None for fallback
            if e.http_status == HTTPStatus.NOT_FOUND.value or (
                e.code == APIErrorCode.INVALID_RESPONSE.value and "collateral" in e.message
            ):
                logger.debug(
                    "collateral_endpoint_unavailable",
                    exchange=self._exchange_name,
                    http_status=e.http_status,
                    error_code=e.code,
                    subaccount_id=subaccount_id,
                    message="Collateral endpoint not available or invalid",
                )
                return None
            # Re-raise other API errors
            raise
        except TransformationError as e:
            logger.exception(
                "transform_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Failed to transform enhanced data",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform enhanced collateral data.",
                original_exception=e,
            ) from e
        except Exception as e:
            logger.exception(
                "unexpected_enhanced_account_failure",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Unexpected enhanced account failure",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected enhanced account service failure.",
                original_exception=e,
            ) from e
        else:
            return internal_summary

    async def _get_basic_account_info(
        self,
        subaccount_id: int | None = None,
    ) -> MarginAccountSummary:
        """Basic account info using legacy endpoints.

        Falls back to original implementation when collateral endpoint
        is unavailable or returns errors.

        Args:
            subaccount_id: Optional subaccount identifier

        Returns:
            Basic MarginAccountSummary

        Raises:
            APIError: If API request fails, data transformation fails, or validation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "_get_basic_account_info"

        try:
            if subaccount_id is not None:
                logger.warning(
                    "subaccount_filtering_not_supported",
                    exchange=self._exchange_name,
                    subaccount_id=subaccount_id,
                    message="Subaccount filtering not supported in basic mode",
                )

            logger.debug(
                "fetching_basic_account_info",
                exchange=self._exchange_name,
                method=current_method,
                message="Fetching basic account info",
            )

            # Fetch components sequentially
            raw_settings = await self._get_raw_account_summary_obj()
            raw_balances_dict = await self._get_raw_balances_dict()
            raw_positions_list = await self._get_raw_positions_list()

            # Transform using original mapper method
            internal_summary = self._mapper.transform_raw_account_summary_to_internal(
                raw_settings=raw_settings,
                spot_balances_raw=raw_balances_dict,
                derivative_positions_raw=raw_positions_list,
            )

            logger.debug(
                "basic_account_summary_created",
                exchange=self._exchange_name,
                method=current_method,
                summary_type="basic",
                message="Basic account summary created successfully",
            )

        except APIError:
            raise
        except TransformationError as e:
            logger.exception(
                "transform_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Failed to transform basic data",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform basic account data.",
                original_exception=e,
            ) from e
        except ValidationError as e:
            logger.exception(
                "validation_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Internal data validation failed",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
            ) from e
        except (ValueError, TypeError) as e:
            logger.exception(
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Service internal logic error",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
            ) from e
        except Exception as e:
            logger.exception(
                "unexpected_basic_account_failure",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Unexpected basic account failure",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected basic account service failure.",
                original_exception=e,
            ) from e
        else:
            return internal_summary

    async def _get_raw_account_summary_obj(self) -> BackpackRawAccountSummaryResponse:
        """Helper to fetch and validate the raw account summary object.

        Returns:
            BackpackRawAccountSummaryResponse object

        Raises:
            APIError: If API request fails
        """
        endpoint_path = "/api/v1/account"
        params = self._request_builder.build_get_account_info_params()

        logger.debug(
            "requesting_raw_account_summary",
            exchange=self._exchange_name,
            endpoint_path=endpoint_path,
            params=params,
            message="Requesting raw account summary from endpoint",
        )

        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params.model_dump(exclude_none=True),
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.SIGNED,
                    endpoint_group="private",
                    request_weight=1,
                ),
            )

            logger.debug(
                "raw_account_summary_response",
                exchange=self._exchange_name,
                status_code=status_code,
                message="Raw account summary response received",
            )

            # Use centralized validation
            validated_data = ensure_dict_response(raw_data, "account summary", status_code)
            return self._response_handler.handle_get_account_info_response(
                validated_data,
                status_code,
            )
        except Exception as e:
            logger.exception(
                "account_summary_fetch_failed",
                exchange=self._exchange_name,
                error=str(e),
                message="Failed to fetch account summary",
            )
            raise APIError(
                message=f"Failed to fetch account summary: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e

    async def _get_raw_collateral_response(
        self,
        subaccount_id: int | None = None,
    ) -> BackpackRawCollateralResponse:
        """Fetch raw collateral data from /api/v1/capital/collateral endpoint.

        Args:
            subaccount_id: Optional subaccount ID (uint16, 0-65535)

        Returns:
            Validated collateral response
        """
        try:
            # Build query parameters
            query_params = self._request_builder.build_collateral_query_params(
                str(subaccount_id) if subaccount_id is not None else None,
            )

            # Make HTTP request
            raw_data, status_code, headers = await self._http_client_requester(
                method="GET",
                endpoint="/api/v1/capital/collateral",
                params=query_params.model_dump(by_alias=True, exclude_none=True),
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.SIGNED,
                    endpoint_group="private",
                    request_weight=1,
                ),
            )

            # Validate response
            self._validate_collateral_response(raw_data, status_code)

            # Type narrowing: after validation, raw_data cannot be None
            if raw_data is None:
                self._raise_unreachable_none_error("raw_data")

            # After validation, raw_data is confirmed to be ParsedJsonResponse
            return self._response_handler.handle_get_collateral_response(
                raw_response_content=raw_data,
                subaccount_id=subaccount_id,
                status_code=status_code,
                headers=headers,
            )

        except Exception as e:
            logger.exception(
                "collateral_fetch_failed",
                exchange=self._exchange_name,
                error=str(e),
                message="Failed to fetch collateral data",
            )
            raise

    async def _get_raw_balances_dict(self) -> dict[str, BackpackRawBalanceResponse]:
        """Helper to fetch and validate raw account balances dictionary.

        Returns:
            Dictionary mapping asset symbols to BackpackRawBalanceResponse objects

        Raises:
            APIError: If API request fails
        """
        if not self._authenticator:
            raise APIError(
                message="Authentication required for fetching balances",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        endpoint_path = "/api/v1/capital"
        params = self._request_builder.build_get_balances_params()

        logger.debug(
            "requesting_raw_balances",
            exchange=self._exchange_name,
            endpoint_path=endpoint_path,
            message="Requesting raw balances dict from endpoint",
        )

        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params.model_dump(exclude_none=True),
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.SIGNED,
                    endpoint_group="private",
                    request_weight=1,
                ),
            )

            logger.debug(
                "raw_balances_response",
                exchange=self._exchange_name,
                status_code=status_code,
                message="Raw balances dict response received",
            )

            # Use centralized validation
            validated_data = ensure_dict_response(raw_data, "balances", status_code)
            return self._response_handler.handle_get_balances_response(validated_data, status_code)
        except Exception as e:
            logger.exception(
                "balances_fetch_failed",
                exchange=self._exchange_name,
                error=str(e),
                message="Failed to fetch balances",
            )
            raise APIError(
                message=f"Failed to fetch balances: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e

    async def _get_raw_positions_list(
        self,
        symbol: str | None = None,
    ) -> list[BackpackRawPositionResponse]:
        """Helper to fetch and validate raw current open positions list.

        Args:
            symbol: Optional symbol filter

        Returns:
            List of BackpackRawPositionResponse objects for current open positions

        Raises:
            APIError: If API request fails
        """
        if not self._authenticator:
            raise APIError(
                message="Authentication required for fetching positions",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        endpoint_path = "/api/v1/position"
        symbol_obj = exchanges.backpack(value=symbol) if symbol else None
        params = self._request_builder.build_get_positions_params(symbol_obj)

        logger.debug(
            "requesting_raw_positions",
            exchange=self._exchange_name,
            endpoint_path=endpoint_path,
            symbol=symbol or "all",
            message="Requesting raw positions from endpoint",
        )

        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params.model_dump(exclude_none=True),
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.SIGNED,
                    endpoint_group="private",
                    request_weight=1,
                ),
            )

            logger.debug(
                "raw_positions_response",
                exchange=self._exchange_name,
                symbol=symbol or "all",
                status_code=status_code,
                message="Raw positions response received",
            )

            # Positions can be dict or list
            if raw_data is None:
                return []

            # Validate response type
            self._validate_positions_response_type(raw_data, status_code)

            # Handle based on type
            if is_dict_response(raw_data):
                return self._response_handler.handle_get_positions_response(
                    raw_data,
                    symbol_obj,
                    status_code,
                )
            if is_list_response(raw_data):
                # Must be list type after validation
                return self._response_handler.handle_get_positions_response(
                    raw_data,
                    symbol_obj,
                    status_code,
                )
            # This should never happen after validation
            self._raise_unreachable_type_error("raw_data")

        except APIError as e:
            # Handle 404 for positions endpoint
            if e.http_status == HTTPStatus.NOT_FOUND.value:
                logger.info(
                    "positions_endpoint_not_found",
                    exchange=self._exchange_name,
                    symbol=symbol or "all",
                    message="Positions 404, returning empty list",
                )
                return []
            raise

    def _validate_collateral_response(
        self,
        raw_data: ParsedJsonResponse | None,
        status_code: int,
    ) -> None:
        """Validate collateral response data.

        Args:
            raw_data: Raw response data
            status_code: HTTP status code

        Raises:
            EmptyResponseError: If response is None
        """
        if raw_data is None:
            raise EmptyResponseError(
                response_type="data",
                operation="collateral request",
                http_status=status_code,
                exchange=self._exchange_name,
            )

    def _validate_positions_response_type(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int,
    ) -> None:
        """Validate positions response type.

        Args:
            raw_data: Raw response data
            status_code: HTTP status code

        Raises:
            APIError: If response type is invalid
        """
        if not (is_dict_response(raw_data) or is_list_response(raw_data)):
            raise APIError(
                message=f"Unexpected response type for positions: {type(raw_data).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

    def _raise_unreachable_none_error(self, variable_name: str) -> NoReturn:
        """Raise UnreachableCodeError for None after validation.

        Args:
            variable_name: Name of the variable that is None

        Raises:
            UnreachableCodeError: Always raises
        """
        raise UnreachableCodeError

    def _raise_unreachable_type_error(self, variable_name: str) -> NoReturn:
        """Raise UnreachableCodeError for invalid type after validation.

        Args:
            variable_name: Name of the variable with invalid type

        Raises:
            UnreachableCodeError: Always raises
        """
        raise UnreachableCodeError
