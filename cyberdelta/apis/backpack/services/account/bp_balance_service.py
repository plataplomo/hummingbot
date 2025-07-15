"""Backpack Balance Service.

This service handles all balance-related operations for the Backpack exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Spot balance retrieval and processing
- Collateral information integration
- Auto-lending scenario handling
- Balance data transformation and validation
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackBalanceMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.protocols.builder_protocols import AccountRequestBuilderProtocol
from cyberdelta.apis.backpack.protocols.handler_protocols import AccountResponseHandlerProtocol
from cyberdelta.apis.backpack.protocols.mapper_protocols import BalanceMapperProtocol
from cyberdelta.apis.backpack.services.account.bp_account_state_service import (
    BackpackAccountStateService,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.utils import ensure_dict_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import SpotBalance
from cyberdelta.core.models.spot_balance import BackpackSpotBalanceDetails
from cyberdelta.utils.parsing import parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackBalanceService:
    """Focused service for Backpack balance operations.

    Handles retrieval, validation, and transformation of balance data
    with comprehensive error handling and collateral integration.
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
        mapper: BalanceMapperProtocol | None = None,
    ) -> None:
        """Initialize the balance service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            account_state_service: Optional shared account state service
                (creates default if not provided)
            mapper: Optional balance mapper instance for dependency injection
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
        self._mapper = mapper or BackpackBalanceMapper()

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Fetch account balances for all assets.

        This method now uses the shared account state service following the Hyperliquid
        clearinghouse pattern. It first tries to get standard balances and falls back
        to collateral data for auto-lending scenarios.

        Returns:
            Dictionary mapping asset symbols to SpotBalance objects

        Raises:
            APIError: If API request fails or data transformation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_balances"

        try:
            # Try standard balance retrieval first
            try:
                logger.debug(
                    "attempting_standard_balance_retrieval",
                    exchange=self._exchange_name,
                    method=current_method,
                    message="Attempting standard balance retrieval",
                )

                # Get raw balances via traditional endpoint
                raw_balances = await self._get_raw_balances_dict()
                internal_balances = await self._transform_raw_balances(raw_balances)

                # Check if auto-lending is active (all spot balances are zero)
                all_balances_zero = all(
                    balance.total_quantity == Decimal(0) for balance in internal_balances.values()
                )

                if all_balances_zero and len(internal_balances) > 0:
                    logger.info(
                        "auto_lending_detected_using_shared_state",
                        exchange=self._exchange_name,
                        method=current_method,
                        message=(
                            "All spot balances are zero, "
                            "using shared account state for collateral data"
                        ),
                    )
                    # Use shared state service for collateral data (following Hyperliquid pattern)
                    return await self._get_balances_from_shared_state()
                # Enhance existing balances with collateral information from shared state
                return await self._enhance_balances_with_shared_state(internal_balances)

            except APIError as e:
                if e.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
                    logger.info(
                        "standard_balance_endpoint_unavailable_using_shared_state",
                        exchange=self._exchange_name,
                        method=current_method,
                        message="Standard balance endpoint unavailable, using shared account state",
                    )
                    # Fallback to shared state service (following Hyperliquid clearinghouse pattern)
                    return await self._get_balances_from_shared_state()
                raise

        except APIError:
            raise
        except (ValidationError, TransformationError) as e:
            self._handle_balance_exception(e, current_method)
            raise  # Re-raise after handling

    async def _get_balances_from_shared_state(self) -> dict[str, SpotBalance]:
        """Get balances from shared account state service.

        This method follows the Hyperliquid clearinghouse pattern of using
        a shared state service to get comprehensive account data.

        Returns:
            Dictionary mapping asset symbols to SpotBalance objects
        """
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name if frame is not None else "_get_balances_from_shared_state"
        )

        logger.debug(
            "fetching_balances_from_shared_state",
            exchange=self._exchange_name,
            method=current_method,
            message="Fetching balances from shared account state service",
        )

        # Get account state from shared service (equivalent to Hyperliquid's clearinghouse call)
        account_state = await self._account_state_service.get_account_state()

        # Transform collateral data to balances
        # (similar to Hyperliquid's clearinghouse transformation)
        internal_balances: dict[str, SpotBalance] = {}
        if account_state.collateral:
            for collateral_asset in account_state.collateral:
                symbol = collateral_asset.symbol
                balance = self._mapper.create_balance_from_collateral(
                    symbol=symbol,
                    collateral_data=collateral_asset,
                    exchange_name=self._exchange_name,
                )
                internal_balances[symbol] = balance

        logger.info(
            "balances_retrieved_from_shared_state",
            exchange=self._exchange_name,
            method=current_method,
            balance_count=len(internal_balances),
            message="Successfully retrieved balances from shared account state",
        )

        return internal_balances

    async def _enhance_balances_with_shared_state(
        self, balances: dict[str, SpotBalance]
    ) -> dict[str, SpotBalance]:
        """Enhance balance data with collateral information from shared state.

        This method uses the shared account state service to get collateral
        information, following the Hyperliquid pattern of centralizing state access.

        Args:
            balances: Dictionary of basic balance objects

        Returns:
            Dictionary of enhanced balance objects with collateral data
        """
        try:
            # Get collateral information from shared state service
            account_state = await self._account_state_service.get_account_state()

            if not account_state or not account_state.collateral:
                logger.debug(
                    "no_collateral_data_from_shared_state",
                    exchange=self._exchange_name,
                    message=(
                        "No collateral data available from shared state, returning basic balances"
                    ),
                )
                return balances

            # Process each balance with collateral data from shared state
            for symbol, balance in balances.items():
                # Find collateral data for this symbol in the list
                collateral_data = None
                for asset in account_state.collateral:
                    if asset.symbol == symbol:
                        collateral_data = asset
                        break

                if collateral_data:
                    # Calculate enhanced fields
                    collateral_weight = parse_decimal_value(
                        collateral_data.collateral_weight,
                        allow_none=False,
                        field_name="collateralWeight",
                    )

                    # Create enhanced details
                    enhanced_details = BackpackSpotBalanceDetails(
                        collateral_weight=collateral_weight,
                        lend_quantity=parse_decimal_value(
                            collateral_data.lend_quantity,
                            allow_none=True,
                            field_name="lend_quantity",
                        ),
                        open_order_quantity=parse_decimal_value(
                            collateral_data.open_order_quantity,
                            allow_none=True,
                            field_name="open_order_quantity",
                        ),
                    )

                    # Update balance with enhanced details
                    balance_dict = balance.model_dump()
                    balance_dict["bp_details"] = enhanced_details

                    # When collateral data is available, update total_quantity to match
                    # the actual total from collateral (which includes lent amounts)
                    collateral_total = parse_decimal_value(
                        collateral_data.total_quantity,
                        allow_none=False,
                        field_name="total_quantity",
                    )
                    balance_dict["total_quantity"] = collateral_total

                    balances[symbol] = secure_transform(
                        data=balance_dict,
                        model_class=SpotBalance,
                        context=f"enhance_balance_from_shared_state_{symbol}",
                        source_exchange=self._exchange_name,
                    )

        except (ValidationError, TransformationError) as e:
            logger.warning(
                "shared_state_enhancement_failed",
                exchange=self._exchange_name,
                error=str(e),
                message=(
                    "Failed to enhance balances with shared state data, returning basic balances"
                ),
            )

        return balances

    async def _get_raw_balances_dict(self) -> dict[str, BackpackRawBalance]:
        """Fetch raw balance data from the API.

        Returns:
            Dictionary mapping symbols to raw balance objects

        Raises:
            APIError: If API request fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "_get_raw_balances_dict"

        if not self._authenticator:
            raise APIError(
                message="Authentication required for fetching balances",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        try:
            # Build request
            request_params = self._request_builder.build_get_balances_params()

            # Execute request
            raw_response, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint="/api/v1/capital",
                params=request_params.model_dump(exclude_none=True),
                is_signed=True,
            )

            # Ensure dict response
            raw_data = ensure_dict_response(raw_response, "balances", status_code)

            # Handle response
            raw_balances = self._response_handler.handle_get_balances_response(
                raw_data, status_code
            )

            if not raw_balances:
                logger.warning(
                    "no_balances_found",
                    exchange=self._exchange_name,
                    method=current_method,
                    message="No balance data returned from exchange",
                )
                raw_balances = {}

        except APIError:
            raise
        except (ValidationError, TransformationError) as e:
            logger.exception(
                "get_raw_balances_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Failed to get raw balances",
            )
            raise APIError(
                message="Failed to retrieve balance data",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

        return raw_balances

    async def _transform_raw_balances(
        self,
        raw_balances: dict[str, BackpackRawBalance],
    ) -> dict[str, SpotBalance]:
        """Transform raw balance data to internal models.

        Args:
            raw_balances: Dictionary of raw balance objects

        Returns:
            Dictionary mapping symbols to SpotBalance objects
        """
        internal_balances: dict[str, SpotBalance] = {}
        transform_errors: list[str] = []

        for symbol, raw_balance in raw_balances.items():
            try:
                internal_balance = self._mapper.transform_raw_balance_to_internal(
                    symbol, raw_balance
                )
                internal_balances[symbol] = internal_balance

            except TransformationError as e:
                error_msg = f"Failed to transform balance for {symbol}: {e}"
                transform_errors.append(error_msg)
                logger.exception(
                    "balance_transform_error",
                    exchange=self._exchange_name,
                    symbol=symbol,
                    error=str(e),
                    message=error_msg,
                )
                continue

        if transform_errors and not internal_balances:
            raise TransformationError(
                message="Failed to transform any balance data",
                source_data={"errors": transform_errors},
            )

        return internal_balances

    async def _enhance_balances_with_collateral(
        self,
        balances: dict[str, SpotBalance],
    ) -> dict[str, SpotBalance]:
        """Enhance balance data with collateral information.

        Args:
            balances: Dictionary of basic balance objects

        Returns:
            Dictionary of enhanced balance objects with collateral data
        """
        try:
            # Get collateral information
            collateral_response = await self._get_raw_collateral_response()

            if not collateral_response:
                logger.debug(
                    "no_collateral_data",
                    exchange=self._exchange_name,
                    message="No collateral data available, returning basic balances",
                )
                return balances

            # Process each balance with collateral data
            for symbol, balance in balances.items():
                # Find collateral data for this symbol in the list
                collateral_data = None
                for asset in collateral_response.collateral:
                    if asset.symbol == symbol:
                        collateral_data = asset
                        break

                if collateral_data:
                    # Calculate enhanced fields
                    collateral_weight = parse_decimal_value(
                        collateral_data.collateral_weight,
                        allow_none=False,
                        field_name="collateralWeight",
                    )
                    # Collateral weight has been parsed and validated

                    # Create enhanced details
                    enhanced_details = BackpackSpotBalanceDetails(
                        collateral_weight=collateral_weight,
                        lend_quantity=parse_decimal_value(
                            collateral_data.lend_quantity,
                            allow_none=True,
                            field_name="lend_quantity",
                        ),
                        open_order_quantity=parse_decimal_value(
                            collateral_data.open_order_quantity,
                            allow_none=True,
                            field_name="open_order_quantity",
                        ),
                    )

                    # Update balance with enhanced details
                    balance_dict = balance.model_dump()
                    balance_dict["bp_details"] = enhanced_details

                    balances[symbol] = secure_transform(
                        data=balance_dict,
                        model_class=SpotBalance,
                        context=f"enhance_balance_{symbol}",
                        source_exchange=self._exchange_name,
                    )

        except (ValidationError, TransformationError) as e:
            logger.warning(
                "collateral_enhancement_failed",
                exchange=self._exchange_name,
                error=str(e),
                message="Failed to enhance balances with collateral data, returning basic balances",
            )

        return balances

    async def _get_raw_collateral_response(self) -> BackpackRawCollateralResponse | None:
        """Get raw collateral information from the API.

        Returns:
            Raw collateral response object or None if unavailable
        """
        try:
            # Build request for collateral endpoint
            endpoint = "/api/v1/capital/collateral"
            params: dict[str, str] = {}

            raw_response, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint,
                params=params,
                is_signed=True,
            )

            raw_data = ensure_dict_response(raw_response, "collateral", status_code)

            collateral_result = self._response_handler.handle_get_collateral_response(
                raw_data,
                None,  # subaccount_id
                status_code,
                {},  # headers
            )

        except APIError as e:
            if e.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
                logger.debug(
                    "collateral_not_available",
                    exchange=self._exchange_name,
                    message="Collateral endpoint not available",
                )
                collateral_result = None
            else:
                raise
        except (ValidationError, TransformationError) as e:
            logger.exception(
                "get_collateral_failed",
                exchange=self._exchange_name,
                error=str(e),
                message="Failed to get collateral data",
            )
            collateral_result = None

        return collateral_result

    async def _handle_auto_lending_scenario(
        self,
        error: APIError,
        current_method: str,
    ) -> dict[str, SpotBalance]:
        """Handle auto-lending scenario where standard balance endpoint fails.

        Args:
            error: The original API error
            current_method: Name of the calling method

        Returns:
            Dictionary of balances or empty dict if recovery fails
        """
        # Initialize recovery result
        recovery_result: dict[str, SpotBalance] | None = None

        if error.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
            logger.info(
                "auto_lending_scenario_detected",
                exchange=self._exchange_name,
                method=current_method,
                message=(
                    "Standard balance endpoint not available, attempting collateral-based approach"
                ),
            )

            try:
                # Get collateral data as alternative
                collateral_response = await self._get_raw_collateral_response()

                if collateral_response and collateral_response.collateral:
                    # Create balances from collateral data
                    balances: dict[str, SpotBalance] = {}
                    for collateral_data in collateral_response.collateral:
                        symbol = collateral_data.symbol
                        balance = self._mapper.create_balance_from_collateral(
                            symbol=symbol,
                            collateral_data=collateral_data,
                            exchange_name=self._exchange_name,
                        )
                        balances[symbol] = balance
                    recovery_result = balances
                else:
                    recovery_result = None

            except (ValidationError, TransformationError) as e:
                logger.exception(
                    "auto_lending_recovery_failed",
                    exchange=self._exchange_name,
                    method=current_method,
                    error=str(e),
                    message="Failed to recover balances from collateral data",
                )
                recovery_result = None

        # Return recovery result if successful, otherwise re-raise original error
        if recovery_result is not None:
            return recovery_result

        raise error

    def _handle_balance_exception(
        self,
        error: Exception,
        current_method: str,
    ) -> dict[str, SpotBalance]:
        """Handle exceptions during balance operations.

        Args:
            error: The exception that occurred
            current_method: Name of the calling method

        Returns:
            Empty dictionary or raises APIError
        """
        if isinstance(error, TransformationError):
            logger.error(
                "balance_transformation_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(error),
                message="Failed to transform balance data",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process balance data",
                original_exception=error,
            ) from error

        if isinstance(error, ValidationError):
            logger.error(
                "balance_validation_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(error),
                message="Balance data validation failed",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Invalid balance data format",
                original_exception=error,
            ) from error

        logger.error(
            "unexpected_balance_error",
            exchange=self._exchange_name,
            method=current_method,
            error=str(error),
            message="Unexpected error during balance operation",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected error retrieving balances",
            original_exception=error,
        ) from error
