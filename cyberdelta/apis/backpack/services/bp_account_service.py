"""CyberDeltaEngine: Backpack Account Service.

------------------------------------------

This service encapsulates the logic for fetching and managing account-specific
information from the Backpack Exchange. It uses the HttpClient, BackpackRequestBuilder,
BackpackResponseHandler, and BackpackAccountDataMapper to interact with the API
and returns Internal Domain Models.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from http import HTTPStatus
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler, RawJsonResponse
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.models.service_args_models import (
    GetMaxBorrowQuantityArgs,
    GetMaxOrderQuantityArgs,
    GetMaxWithdrawalQuantityArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    TransferArgs,
    UpdateAccountSettingsArgs,
    WithdrawArgs,
)
from cyberdelta.apis.utils import ensure_dict_response, ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    AccountSettings,
    DerivativePosition,
    MarginAccountSummary,  # Reverted to MarginAccountSummary
    Order,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.operations import Transfer, Withdrawal
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform
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


class BackpackAccountService:
    """Service class for Backpack account management operations.

    Returns Internal Domain Models.
    """

    _http_client_requester: HttpClientRequesterSig
    _request_builder: BackpackRequestBuilder
    _response_handler: BackpackResponseHandler
    _mapper: BackpackAccountDataMapper
    _authenticator: IAuthenticator | None
    _exchange_name: str

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        mapper: BackpackAccountDataMapper | None = None,
    ) -> None:
        """Initialize the BackpackAccountService.

        Args:
            http_client_requester: A callable for making API requests.
            request_builder: An instance of BackpackRequestBuilder.
            response_handler: An instance of BackpackResponseHandler.
            authenticator: An instance of IAuthenticator for signed requests.
            exchange_name: The name of the exchange.
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

    async def _get_raw_balances_dict(self) -> dict[str, BackpackRawBalance]:
        """Helper to fetch and validate raw account balances dictionary.

        Returns:
            Dictionary mapping asset symbols to BackpackRawBalance objects.
        """
        endpoint_path = "/api/v1/capital"
        params = self._request_builder.build_get_balances_params()
        logger.debug(
            "requesting_raw_balances: Requesting raw balances dict from endpoint",
            exchange_name=self._exchange_name,
            endpoint_path=endpoint_path,
            params=params,
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        # Not strictly needed if not used beyond _http_client_requester
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params.model_dump(),
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )
            logger.debug(
                "raw_balances_response: Raw balances dict response received",
                exchange_name=self._exchange_name,
                raw_data=raw_data,
                status_code=status_code,
            )
            # Use centralized validation
            validated_data = ensure_dict_response(raw_data, "balances", status_code)
            return self._response_handler.handle_get_balances_response(validated_data, status_code)
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                "validation_error_balances: Validation/map error for raw balances dict",
                error=str(e_val),
                raw_data=raw_data,
                status_code=status_code,
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
            logger.exception(
                "unhandled_error_balances: Unhandled error for raw balances dict",
                error=str(e_unhandled),
                raw_info=raw_info_for_log,
                status_code=status_code,
            )
            raise APIError(
                message=f"Unexpected error for raw balances dict: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def _get_raw_positions_list(self, symbol: str | None = None) -> list[BackpackRawPosition]:
        """Helper to fetch and validate raw current open positions list.

        Returns:
            List of BackpackRawPosition objects for current open positions.
        """
        endpoint_path = "/api/v1/position"
        params = self._request_builder.build_get_positions_params(symbol)
        logger.debug(
            "requesting_raw_positions: Requesting raw positions from endpoint",
            exchange_name=self._exchange_name,
            endpoint_path=endpoint_path,
            symbol=symbol or "all",
            params=params,
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params.model_dump(),
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )
            logger.debug(
                "raw_positions_response: Raw positions response received",
                exchange_name=self._exchange_name,
                symbol=symbol or "all",
                raw_data=raw_data,
                status_code=status_code,
            )
            # Use centralized validation - positions can be dict or list
            # The response_handler.handle_get_positions_response now correctly handles
            # dict for single symbol or list for all symbols.
            # Check the actual type of the response, not whether symbol was provided
            if is_dict_response(raw_data):
                # raw_data is now typed as dict[str, Any]
                return self._response_handler.handle_get_positions_response(
                    raw_data,
                    symbol,
                    status_code,
                )
            if is_list_response(raw_data):
                # raw_data is now typed as list[Any]
                return self._response_handler.handle_get_positions_response(
                    raw_data,
                    symbol,
                    status_code,
                )
            # Handle unexpected response type
            raise APIError(
                message=f"Unexpected response type for positions: {type(raw_data).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )
        except APIError as e:
            # Handle 404 for positions endpoint - Backpack may not support this endpoint
            # or account may have no positions, return empty list
            if e.http_status == HTTPStatus.NOT_FOUND.value:
                logger.info(
                    "positions_endpoint_not_found: Positions 404, returning empty list",
                    exchange_name=self._exchange_name,
                    symbol=symbol or "all",
                )
                return []
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                "validation_error_positions: Validation/map error for raw positions",
                symbol=symbol or "all",
                error=str(e_val),
                raw_data=raw_data,
                status_code=status_code,
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
            logger.exception(
                "unhandled_error_positions: Unhandled error for raw positions",
                symbol=symbol or "all",
                error=str(e_unhandled),
                raw_info=raw_info_for_log,
                status_code=status_code,
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
        params = self._request_builder.build_get_account_info_params()
        logger.debug(
            "requesting_raw_account_summary: Requesting raw account summary from endpoint",
            exchange_name=self._exchange_name,
            endpoint_path=endpoint_path,
            params=params,
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params.model_dump(),
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )
            logger.debug(
                "raw_account_summary_response: Raw account summary response received",
                exchange_name=self._exchange_name,
                raw_data=raw_data,
                status_code=status_code,
            )
            # Use centralized validation
            validated_data = ensure_dict_response(raw_data, "account summary", status_code)
            return self._response_handler.handle_get_account_info_response(
                validated_data,
                status_code,
            )
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                "validation_error_account_summary: Validation/map error for raw account summary",
                error=str(e_val),
                raw_data=raw_data,
                status_code=status_code,
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
            logger.exception(
                "unhandled_error_account_summary: Unhandled error for raw account summary",
                error=str(e_unhandled),
                raw_info=raw_info_for_log,
                status_code=status_code,
            )
            raise APIError(
                message=f"Unexpected error for raw account summary: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Retrieves all spot balances from the account.

        Note: Handles Backpack's auto-lending feature where spot balances may show
        zero when funds are auto-lent. When all spot balances are zero, this method
        automatically fetches collateral data to provide complete balance information
        including lent amounts in the bp_details extension slot.
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
                "getting_account_balances",
                action="get_balances",
                exchange=self._exchange_name,
                message=f"[{self._exchange_name}] Getting account balances.",
            )
            raw_balances_payload = await self._get_raw_balances_dict()
            internal_balances: dict[str, SpotBalance] = {}

            # Transform basic spot balances
            for asset_symbol, raw_balance_model in raw_balances_payload.items():
                try:
                    internal_balances[asset_symbol] = (
                        self._mapper.transform_raw_balance_to_internal(
                            asset_symbol,
                            raw_balance_model,
                        )
                    )
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        "skipping_balance_mapping: Skipping balance mapping for asset due to error",
                        exchange_name=self._exchange_name,
                        asset_symbol=asset_symbol,
                        error=str(e_map_item),
                        raw_balance_model=raw_balance_model,
                    )

            # Check if auto-lending is potentially active (all spot balances are zero)
            all_spot_balances_zero = all(
                balance.total_quantity == Decimal(0) for balance in internal_balances.values()
            )

            if all_spot_balances_zero and len(internal_balances) > 0:
                logger.info(
                    "checking_collateral_for_auto_lending: All balances zero, checking collateral",
                    exchange_name=self._exchange_name,
                )

                try:
                    # Fetch collateral data to get the complete picture
                    raw_collateral = await self._get_raw_collateral_response()

                    # Enhance balances with collateral information
                    internal_balances = await self._enhance_balances_with_collateral(
                        spot_balances=internal_balances,
                        collateral_response=raw_collateral,
                    )

                    logger.debug(
                        "enhanced_balances_with_collateral: Enhanced balances with collateral data",
                        exchange_name=self._exchange_name,
                    )
                except APIError as e_collateral:
                    # If collateral endpoint fails, log but continue with spot balances
                    logger.warning(
                        "collateral_fetch_failed: Failed to fetch collateral, continuing with spot",
                        exchange_name=self._exchange_name,
                        error=str(e_collateral),
                    )

            logger.debug(
                "balances_mapped_successfully: Successfully mapped spot balances",
                exchange_name=self._exchange_name,
                balance_count=len(internal_balances),
            )
            return internal_balances

        except APIError:
            # Re-raise APIErrors from _get_raw_balances_dict, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_failed: Failed to transform exchange data",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_transform),
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
                "validation_failed: Internal data validation failed",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_val),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "service_logic_error: Service internal logic error",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_service_logic),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure: Unexpected service failure",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def _enhance_balances_with_collateral(
        self,
        spot_balances: dict[str, SpotBalance],
        collateral_response: BackpackRawCollateralResponse,
    ) -> dict[str, SpotBalance]:
        """Enhance spot balances with collateral data for auto-lending scenario.

        Args:
            spot_balances: Original spot balances (likely all zeros with auto-lending)
            collateral_response: Raw collateral response containing lent balances

        Returns:
            Enhanced spot balances with lend_quantity populated in bp_details
        """
        enhanced_balances = spot_balances.copy()

        if not collateral_response.collateral:
            return enhanced_balances

        # Create a mapping of collateral data by symbol
        collateral_by_symbol = {asset.symbol: asset for asset in collateral_response.collateral}

        # Enhance existing spot balances with collateral data
        for asset_symbol, spot_balance in spot_balances.items():
            collateral_asset = collateral_by_symbol.get(asset_symbol)
            if collateral_asset:
                # Parse collateral amounts
                from cyberdelta.utils.parsing import parse_decimal_value

                total_quantity = parse_decimal_value(
                    collateral_asset.total_quantity,
                    allow_none=False,
                    field_name=f"{asset_symbol}_collateral_total",
                )
                lend_quantity = parse_decimal_value(
                    collateral_asset.lend_quantity,
                    allow_none=False,
                    field_name=f"{asset_symbol}_lend_quantity",
                )
                open_order_quantity = parse_decimal_value(
                    collateral_asset.open_order_quantity,
                    allow_none=True,
                    field_name=f"{asset_symbol}_open_order_quantity",
                ) or Decimal(0)

                if total_quantity and total_quantity > Decimal(0):
                    # Create enhanced bp_details with lend_quantity
                    from cyberdelta.core.models.spot_balance import BackpackSpotBalanceDetails

                    enhanced_bp_details = BackpackSpotBalanceDetails(
                        open_order_quantity=open_order_quantity,
                        lend_quantity=lend_quantity,
                    )

                    # Create new SpotBalance with collateral data as the true balance
                    # (spot + collateral, but spot is usually 0 in auto-lending scenario)
                    true_total = spot_balance.total_quantity + total_quantity
                    true_available = spot_balance.available_quantity + (
                        total_quantity - open_order_quantity
                    )

                    # Use secure_transform for type-safe model creation
                    balance_data = {
                        "exchange": spot_balance.exchange,
                        "asset": spot_balance.asset,
                        "timestamp": spot_balance.timestamp.isoformat(),
                        "total_quantity": str(true_total),
                        "available_quantity": str(true_available),
                        "bp_details": (
                            enhanced_bp_details.model_dump() if enhanced_bp_details else None
                        ),
                    }
                    enhanced_balances[asset_symbol] = secure_transform(
                        data=balance_data,
                        model_class=SpotBalance,
                        context=f"enhance_balance_{asset_symbol}",
                        source_exchange="backpack",
                    )

        # Add any assets that exist only in collateral (not in spot response)
        for asset_symbol, collateral_asset in collateral_by_symbol.items():
            if asset_symbol not in enhanced_balances:
                from datetime import UTC, datetime

                from cyberdelta.core.models.spot_balance import BackpackSpotBalanceDetails
                from cyberdelta.utils.parsing import parse_decimal_value

                total_quantity = parse_decimal_value(
                    collateral_asset.total_quantity,
                    allow_none=False,
                    field_name=f"{asset_symbol}_collateral_total",
                )
                lend_quantity = parse_decimal_value(
                    collateral_asset.lend_quantity,
                    allow_none=False,
                    field_name=f"{asset_symbol}_lend_quantity",
                )
                open_order_quantity = parse_decimal_value(
                    collateral_asset.open_order_quantity,
                    allow_none=True,
                    field_name=f"{asset_symbol}_open_order_quantity",
                ) or Decimal(0)

                if total_quantity and total_quantity > Decimal(0):
                    enhanced_bp_details = BackpackSpotBalanceDetails(
                        open_order_quantity=open_order_quantity,
                        lend_quantity=lend_quantity,
                    )

                    # Use secure_transform for type-safe model creation
                    balance_data = {
                        "exchange": ExchangeName.BACKPACK.value,
                        "asset": asset_symbol.upper(),
                        "timestamp": datetime.now(UTC).isoformat(),
                        "total_quantity": str(total_quantity),
                        "available_quantity": str(total_quantity - open_order_quantity),
                        "bp_details": (
                            enhanced_bp_details.model_dump() if enhanced_bp_details else None
                        ),
                    }
                    enhanced_balances[asset_symbol] = secure_transform(
                        data=balance_data,
                        model_class=SpotBalance,
                        context=f"collateral_only_balance_{asset_symbol}",
                        source_exchange="backpack",
                    )

        return enhanced_balances

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Retrieves open derivative positions, optionally filtered by symbol."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_positions"

        if symbol is not None and not symbol:
            raise ValueError(
                f"[{current_method}] 'symbol' must be a non-empty string when provided.",
            )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.info(
                "getting_derivative_positions: Getting derivative positions",
                exchange_name=self._exchange_name,
                symbol=symbol or "all",
            )
            raw_positions_list = await self._get_raw_positions_list(symbol)
            internal_positions: list[DerivativePosition] = []
            for raw_position_model in raw_positions_list:
                try:
                    internal_positions.append(
                        self._mapper.transform_raw_position_to_internal(raw_position_model),
                    )
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        "skipping_position_mapping: Skipping position mapping due to error",
                        exchange_name=self._exchange_name,
                        symbol=(
                            raw_position_model.symbol
                            if hasattr(raw_position_model, "symbol")
                            else "UnknownSymbol"
                        ),
                        error=str(e_map_item),
                        raw_position_model=raw_position_model,
                    )
            logger.debug(
                "positions_mapped_successfully: Successfully mapped derivative positions",
                exchange_name=self._exchange_name,
                position_count=len(internal_positions),
            )
            return internal_positions

        except APIError:
            # Re-raise APIErrors from _get_raw_positions_list, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_failed: Failed to transform exchange data for positions",
                exchange_name=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_transform),
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
                "validation_failed: Internal data validation failed for positions",
                exchange_name=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_val),
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
            # This is from service internal logic - wrap as APIError
            logger.exception(
                "service_logic_error: Service internal logic error for positions",
                exchange_name=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_service_logic),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure: Unexpected service failure for positions",
                exchange_name=self._exchange_name,
                method=current_method,
                symbol=symbol or "all",
                error=str(e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_account_summary(self, subaccount_id: int | None = None) -> MarginAccountSummary:
        """Get comprehensive account margin summary.

        CONSISTENCY WITH HYPERLIQUID:
        - Method name aligned with HyperliquidAccountService.get_account_summary()
        - Same return type: MarginAccountSummary
        - Same error handling patterns
        - Enhanced with collateral data when available

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
            "getting_account_summary: Getting account summary",
            exchange_name=self._exchange_name,
            subaccount_id=subaccount_id,
        )

        try:
            # Try enhanced implementation with collateral endpoint
            enhanced_summary = await self._get_enhanced_account_info(subaccount_id)
            if enhanced_summary is not None:
                logger.debug(
                    "enhanced_account_summary_retrieved: Enhanced account summary retrieved",
                    exchange_name=self._exchange_name,
                    total_equity=enhanced_summary.total_equity,
                )
                return enhanced_summary

            # Fallback to basic implementation
            logger.info(
                "using_basic_account_summary: Collateral endpoint unavailable, using basic summary",
                exchange_name=self._exchange_name,
            )
            return await self._get_basic_account_info(subaccount_id)

        except APIError:
            raise
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "service_logic_error: Service logic error",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_service_logic),
            )
            raise APIError(
                code=APIErrorCode.INVALID_REQUEST.value,
                message="Invalid request parameters.",
                original_exception=e_service_logic,
                http_status=400,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure: Unexpected service failure",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
            ) from e_unexpected

    async def _get_enhanced_account_info(
        self,
        subaccount_id: int | None = None,
    ) -> MarginAccountSummary | None:
        """Enhanced account info using collateral endpoint.

        Fetches comprehensive margin data from /api/v1/capital/collateral
        with parallel fetching of positions data for complete account state.
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "_get_enhanced_account_info"

        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.debug(
                "fetching_enhanced_account_data",
                action="get_account_summary",
                exchange=self._exchange_name,
                with_collateral=True,
                message=f"[{self._exchange_name}] Fetching enhanced account data with collateral.",
            )

            # Fetch collateral, settings, and positions data in parallel
            collateral_task = self._get_raw_collateral_response(subaccount_id)
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
                "enhanced_account_summary_created: Enhanced account summary created",
                exchange_name=self._exchange_name,
            )
            return internal_summary

        except APIError as e:
            # If collateral endpoint not available (404) or returns invalid data,
            # return None for fallback
            if e.http_status == HTTPStatus.NOT_FOUND.value or (
                e.code == APIErrorCode.INVALID_RESPONSE.value and "collateral" in e.message
            ):
                logger.debug(
                    "collateral_endpoint_unavailable: Collateral endpoint not available or invalid",
                    exchange_name=self._exchange_name,
                    http_status=e.http_status,
                    error_code=e.code,
                    subaccount_id=subaccount_id,
                )
                return None
            # Re-raise other API errors
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_failed: Failed to transform enhanced data",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_transform),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform enhanced collateral data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_enhanced_account_failure: Unexpected enhanced account failure",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected enhanced account service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def _get_basic_account_info(
        self,
        subaccount_id: int | None = None,
    ) -> MarginAccountSummary:
        """Basic account info using legacy endpoints.

        Falls back to original implementation when collateral endpoint
        is unavailable or returns errors.
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "_get_basic_account_info"

        status_code: int = 0
        raw_response_content: str | None = None

        try:
            if subaccount_id is not None:
                logger.warning(
                    "subaccount_filtering_not_supported: Subaccount filtering not supported",
                    exchange_name=self._exchange_name,
                    subaccount_id=subaccount_id,
                )

            logger.debug(
                "fetching_basic_account_info: Fetching basic account info",
                exchange_name=self._exchange_name,
            )

            # Original implementation: fetch components sequentially
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
                "account_summary_created",
                action="get_account_summary",
                exchange=self._exchange_name,
                summary_type="basic",
                message=f"[{self._exchange_name}] Basic account summary created successfully.",
            )
            return internal_summary

        except APIError:
            # Re-raise APIErrors from _get_raw_* methods, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_failed: Failed to transform basic data",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_transform),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform basic account data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.exception(
                "validation_failed: Internal data validation failed",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_val),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "service_logic_error: Service internal logic error",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_service_logic),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_basic_account_failure: Unexpected basic account failure",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected basic account service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def _get_raw_collateral_response(
        self,
        subaccount_id: int | None = None,
    ) -> BackpackRawCollateralResponse:
        """Fetch raw collateral data from /api/v1/capital/collateral endpoint.

        Args:
            subaccount_id: Optional subaccount ID (uint16, 0-65535)

        Returns:
            Validated collateral response

        Raises:
            APIError: If endpoint call fails or validation fails
        """
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name if frame is not None else "_get_raw_collateral_response"
        )

        try:
            # Build query parameters
            query_params = self._request_builder.build_collateral_query_params(subaccount_id)

            # Make HTTP request using the requester pattern
            raw_data, status_code, headers = await self._http_client_requester(
                method="GET",
                endpoint="/api/v1/capital/collateral",
                params=query_params.model_dump(by_alias=True, exclude_none=True),
                is_signed=True,  # OpenAPI: Optional auth, but we'll use signed
                endpoint_group="private",
                request_weight=1,
            )

            # Validate response
            if raw_data is None:
                raise APIError(
                    message="No data received for collateral request",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            return self._response_handler.handle_get_collateral_response(
                raw_response_content=raw_data,
                subaccount_id=subaccount_id,
                status_code=status_code,
                headers=headers,
            )

        except Exception as e:
            logger.exception(
                "collateral_fetch_failed: Failed to fetch collateral data",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise

    # --- Private Internal Limits Methods (INTERNAL USE ONLY) ---

    async def _get_exchange_max_borrow_quantity(self, symbol: str) -> Decimal:
        """PRIVATE: Fetches max borrow quantity from the exchange for validation.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.
        This method is NOT exposed publicly to maintain exchange agnosticism.

        Args:
            symbol: Asset symbol to check borrow limits for

        Returns:
            Maximum borrow quantity as Decimal

        Raises:
            APIError: If endpoint call fails or validation fails
        """
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name if frame is not None else "_get_exchange_max_borrow_quantity"
        )

        try:
            # Build query parameters
            args = GetMaxBorrowQuantityArgs(symbol=symbol)
            query_params = self._request_builder.build_max_borrow_quantity_params(args)

            # Make HTTP request
            raw_data, status_code, headers = await self._http_client_requester(
                method="GET",
                endpoint="/api/v1/account/limits/borrow",
                params=query_params.model_dump(by_alias=True, exclude_none=True),
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            # Validate response
            if raw_data is None:
                raise APIError(
                    message="No data received for max borrow quantity request",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            raw_response = self._response_handler.handle_max_borrow_quantity_response(
                raw_response_content=raw_data,
                symbol=symbol,
                status_code=status_code,
                headers=headers,
            )

            # Parse and return Decimal value
            max_quantity = parse_decimal_value(
                raw_response.max_borrow_quantity,
                field_name="max_borrow_quantity",
                allow_none=False,
            )
            if max_quantity is None:
                raise APIError(
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    message="Missing max_borrow_quantity in response",
                )

            return max_quantity

        except Exception as e:
            logger.exception(
                "max_borrow_fetch_failed: Failed to fetch max borrow quantity",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise

    async def _get_exchange_max_order_quantity(self, args: GetMaxOrderQuantityArgs) -> Decimal:
        """PRIVATE: Fetches max order quantity from the exchange for validation.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.
        This method is NOT exposed publicly to maintain exchange agnosticism.

        Args:
            args: Validated arguments for max order quantity request

        Returns:
            Maximum order quantity as Decimal

        Raises:
            APIError: If endpoint call fails or validation fails
        """
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name if frame is not None else "_get_exchange_max_order_quantity"
        )

        try:
            # Build query parameters
            query_params = self._request_builder.build_max_order_quantity_params(args)

            # Make HTTP request
            raw_data, status_code, headers = await self._http_client_requester(
                method="GET",
                endpoint="/api/v1/account/limits/order",
                params=query_params.model_dump(by_alias=True, exclude_none=True),
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            # Validate response
            side_str = "Bid" if args.side == OrderSide.BUY else "Ask"
            if raw_data is None:
                raise APIError(
                    message="No data received for max order quantity request",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            raw_response = self._response_handler.handle_max_order_quantity_response(
                raw_response_content=raw_data,
                symbol=args.symbol,
                side=side_str,
                status_code=status_code,
                headers=headers,
            )

            # Parse and return Decimal value
            max_quantity = parse_decimal_value(
                raw_response.max_order_quantity,
                field_name="max_order_quantity",
                allow_none=False,
            )
            if max_quantity is None:
                raise APIError(
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    message="Missing max_order_quantity in response",
                )

            return max_quantity

        except Exception as e:
            logger.exception(
                "max_order_fetch_failed: Failed to fetch max order quantity",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise

    async def _get_exchange_max_withdrawal_quantity(
        self,
        args: GetMaxWithdrawalQuantityArgs,
    ) -> Decimal:
        """PRIVATE: Fetches max withdrawal quantity from the exchange for validation.

        INTERNAL USE ONLY: For risk calculation validation and reconciliation.
        This method is NOT exposed publicly to maintain exchange agnosticism.

        Args:
            args: Validated arguments for max withdrawal quantity request

        Returns:
            Maximum withdrawal quantity as Decimal

        Raises:
            APIError: If endpoint call fails or validation fails
        """
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name if frame is not None else "_get_exchange_max_withdrawal_quantity"
        )

        try:
            # Build query parameters
            query_params = self._request_builder.build_max_withdrawal_quantity_params(args)

            # Make HTTP request
            raw_data, status_code, headers = await self._http_client_requester(
                method="GET",
                endpoint="/api/v1/account/limits/withdrawal",
                params=query_params.model_dump(by_alias=True, exclude_none=True),
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            # Validate response
            if raw_data is None:
                raise APIError(
                    message="No data received for max withdrawal quantity request",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            raw_response = self._response_handler.handle_max_withdrawal_quantity_response(
                raw_response_content=raw_data,
                symbol=args.symbol,
                status_code=status_code,
                headers=headers,
            )

            # Parse and return Decimal value
            max_quantity = parse_decimal_value(
                raw_response.max_withdrawal_quantity,
                field_name="max_withdrawal_quantity",
                allow_none=False,
            )
            if max_quantity is None:
                raise APIError(
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    message="Missing max_withdrawal_quantity in response",
                )

            return max_quantity

        except Exception as e:
            logger.exception(
                "max_withdrawal_fetch_failed: Failed to fetch max withdrawal quantity",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise

    def _validate_account_types(self, args: TransferArgs, current_method: str) -> None:
        """Validate account types for transfer."""
        valid_accounts = {"SPOT", "MARGIN", "FUTURES"}
        if args.from_account_type not in valid_accounts:
            raise ValueError(
                f"[{current_method}] Invalid from_account_type: {args.from_account_type}. "
                f"Must be one of {valid_accounts}",
            )
        if args.to_account_type not in valid_accounts:
            raise ValueError(
                f"[{current_method}] Invalid to_account_type: {args.to_account_type}. "
                f"Must be one of {valid_accounts}",
            )

    async def _execute_transfer_request(self, args: TransferArgs) -> tuple[ParsedJsonResponse, int]:
        """Execute the transfer API request."""
        endpoint_path = "/api/v1/capital/transfer"
        payload = self._request_builder.build_internal_transfer_payload(
            asset_symbol=args.asset,
            amount=args.amount,
            from_account=args.from_account_type,
            to_account=args.to_account_type,
            client_transfer_id=args.client_transfer_id,
        )
        logger.debug(
            "requesting_transfer: Requesting transfer from endpoint",
            exchange_name=self._exchange_name,
            endpoint_path=endpoint_path,
            payload=payload,
        )

        raw_data, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

        logger.debug(
            "raw_transfer_response: Raw transfer response received",
            exchange_name=self._exchange_name,
            raw_data=raw_data,
            status_code=status_code,
        )

        # Use centralized validation
        validated_data = ensure_dict_response(raw_data, "transfer", status_code)

        return validated_data, status_code

    def _process_transfer_response(
        self,
        raw_data: ParsedJsonResponse,
        args: TransferArgs,
        status_code: int,
    ) -> Transfer:
        """Process and transform transfer response."""
        validated_raw_json_response: RawJsonResponse = (
            self._response_handler.handle_transfer_response(raw_data, status_code)
        )

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
            raw_response=validated_raw_json_response,
            exchange_name=self._exchange_name,
            asset=args.asset,
            quantity=args.amount,
            from_account_type_raw=args.from_account_type,
            to_account_type_raw=args.to_account_type,
            client_transfer_id=args.client_transfer_id,
        )

        logger.debug(
            "internal_transfer_mapped",
            action="transfer",
            exchange=self._exchange_name,
            transfer=internal_transfer,
            message=f"[{self._exchange_name}] Mapped internal transfer: {internal_transfer}",
        )
        logger.debug(
            "transfer_successful: Transfer successful",
            exchange_name=self._exchange_name,
            response=internal_transfer.model_dump_json(exclude_none=True),
        )
        return internal_transfer

    def _handle_transfer_exceptions(
        self,
        e: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle various transfer-related exceptions."""
        if isinstance(e, APIError):
            raise
        if isinstance(e, TransformationError):
            logger.exception(
                "transform_failed: Failed to transform exchange data for transfer",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        if isinstance(e, ValidationError):
            logger.exception(
                "validation_failed: Internal data validation failed for transfer",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        if isinstance(e, ValueError | TypeError):
            error_msg = str(e)
            if current_method in error_msg and any(
                param in error_msg for param in ["from_account_type", "to_account_type"]
            ):
                raise
            logger.exception(
                "service_logic_error: Service internal logic error for transfer",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        logger.exception(
            "unexpected_service_failure: Unexpected service failure for transfer",
            exchange_name=self._exchange_name,
            method=current_method,
            error=str(e),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Performs an internal transfer of funds between account types."""
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "transfer"

        self._validate_account_types(args, current_method)

        raw_response_content: str | None = None
        status_code: int = 0

        try:
            raw_data, status_code = await self._execute_transfer_request(args)
            raw_response_content = str(raw_data)
            return self._process_transfer_response(raw_data, args, status_code)
        except Exception as e:
            self._handle_transfer_exceptions(e, current_method, status_code, raw_response_content)
            # DEFENSIVE CHECK: This should never be reached as _handle_transfer_exceptions raises
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in transfer",
                original_exception=e,
                http_status=status_code,
            ) from e

    def _validate_withdrawal_network(self, args: WithdrawArgs, current_method: str) -> None:
        """Validate withdrawal network support."""
        if args.network is None:
            raise ValueError(f"[{current_method}] 'network' is required for withdrawal.")

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

        if args.network not in blockchain_mapping:
            raise ValueError(
                f"[{current_method}] Unsupported network: {args.network}. "
                f"Supported networks: {list(blockchain_mapping.keys())}",
            )

    async def _execute_withdrawal_request(
        self,
        args: WithdrawArgs,
    ) -> tuple[ParsedJsonResponse, int]:
        """Execute withdrawal API request."""
        endpoint_path = "/api/v1/capital/withdrawals"

        # DEFENSIVE CHECK: Ensure network is not None. Mypy=[arg-type]
        if args.network is None:
            raise ValueError("Network is required for withdrawal")

        payload = self._request_builder.build_withdraw_payload(
            asset=args.asset,
            amount=args.amount,
            address=args.address,
            network=args.network,
            tag=args.tag,
            client_withdrawal_id=args.client_withdrawal_id,
            two_factor_token=args.two_factor_token,
        )
        logger.debug(
            "requesting_withdrawal: Requesting withdrawal from endpoint",
            exchange_name=self._exchange_name,
            endpoint_path=endpoint_path,
            payload=payload,
        )

        raw_data, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

        logger.debug(
            "raw_withdrawal_response: Raw withdrawal response received",
            exchange_name=self._exchange_name,
            raw_data=raw_data,
            status_code=status_code,
        )

        # Use centralized validation
        validated_data = ensure_dict_response(raw_data, "withdrawal", status_code)

        return validated_data, status_code

    def _process_withdrawal_response(
        self,
        raw_data: ParsedJsonResponse,
        args: WithdrawArgs,
        status_code: int,
    ) -> Withdrawal:
        """Process and transform withdrawal response."""
        raw_withdrawal_model: BackpackRawWithdrawalResponse = (
            self._response_handler.handle_withdraw_response(raw_data, status_code)
        )

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
            "withdrawal_mapped: Mapped internal withdrawal",
            exchange_name=self._exchange_name,
            internal_withdrawal=internal_withdrawal,
        )
        return internal_withdrawal

    def _handle_withdrawal_exceptions(
        self,
        e: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle various withdrawal-related exceptions."""
        if isinstance(e, APIError):
            raise
        if isinstance(e, TransformationError):
            logger.exception(
                "transform_failed: Failed to transform exchange data for withdrawal",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        if isinstance(e, ValidationError):
            logger.exception(
                "validation_failed: Internal data validation failed for withdrawal",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        if isinstance(e, ValueError | TypeError):
            error_msg = str(e)
            if current_method in error_msg and "network" in error_msg:
                raise
            logger.exception(
                "service_logic_error: Service internal logic error for withdrawal",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        logger.exception(
            "unexpected_service_failure: Unexpected service failure for withdrawal",
            exchange_name=self._exchange_name,
            method=current_method,
            error=str(e),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Initiates a withdrawal of funds to an external address."""
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "withdraw"

        self._validate_withdrawal_network(args, current_method)

        raw_response_content: str | None = None
        status_code: int = 0

        try:
            raw_data, status_code = await self._execute_withdrawal_request(args)
            raw_response_content = str(raw_data)
            return self._process_withdrawal_response(raw_data, args, status_code)
        except Exception as e:
            self._handle_withdrawal_exceptions(e, current_method, status_code, raw_response_content)
            # DEFENSIVE CHECK: This should never be reached as _handle_withdrawal_exceptions raises
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in withdraw",
                original_exception=e,
                http_status=status_code,
            ) from e

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Retrieves historical order data."""
        # Service Input Parameter Validation is now handled by GetOrderHistoryArgs Pydantic model
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_history"

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint_path = "/wapi/v1/history/orders"
            start_time_ms = int(args.start_time.timestamp() * 1000) if args.start_time else None
            end_time_ms = int(args.end_time.timestamp() * 1000) if args.end_time else None

            params = self._request_builder.build_get_order_history_params(
                symbol=args.symbol,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                limit=args.limit,
                order_id=args.order_id,
                client_order_id=args.client_order_id,
            )
            logger.debug(
                "requesting_order_history: Requesting order history from endpoint",
                exchange_name=self._exchange_name,
                endpoint_path=endpoint_path,
                params=params,
            )

            raw_data, status_code, _ = await self._http_client_requester(
                method="GET",
                endpoint=endpoint_path,
                params=params.model_dump(by_alias=True, exclude_none=True),
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                "raw_order_history_response: Raw order history response received",
                exchange_name=self._exchange_name,
                raw_data=raw_data,
                status_code=status_code,
            )
            # Use centralized validation
            validated_data = ensure_list_response(raw_data, "order history", status_code)
            raw_data = validated_data

            raw_orders_list: list[BackpackRawOrder] = (
                self._response_handler.handle_get_order_history_response(
                    raw_data,
                    args.symbol,
                    status_code,
                )
            )

            internal_orders: list[Order] = []
            for raw_order_model in raw_orders_list:
                try:
                    internal_orders.append(
                        self._mapper.transform_raw_order_to_internal(raw_order_model),
                    )
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        "skipping_order_history_mapping: Skipping order mapping due to error",
                        exchange_name=self._exchange_name,
                        order_id=raw_order_model.clientId or raw_order_model.id,
                        error=str(e_map_item),
                        raw_order=raw_order_model.model_dump_json(exclude_none=True),
                    )

            logger.debug(
                "order_history_mapped: Mapped internal order history",
                exchange_name=self._exchange_name,
                order_count=len(internal_orders),
            )
            return internal_orders

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_failed: Failed to transform exchange data for order history",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_transform),
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
                "validation_failed: Internal data validation failed for order history",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_val),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            # Since input parameter validation is now handled by Pydantic model,
            # any ValueError/TypeError here is from service internal logic - wrap as APIError
            logger.exception(
                "service_logic_error: Service internal logic error for order history",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_service_logic),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure: Unexpected service failure for order history",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def _execute_trade_history_request(
        self,
        args: GetTradeHistoryArgs,
    ) -> tuple[ParsedJsonResponse, int]:
        """Execute trade history API request."""
        endpoint_path = "/wapi/v1/history/fills"

        params = self._request_builder.build_get_trade_history_params(
            symbol=args.symbol,
            limit=args.limit,
            start_time_ms=None,  # Not in service signature
            end_time_ms=None,  # Not in service signature
            from_id=None,  # Not in service signature
        )
        logger.debug(
            "requesting_trade_history: Requesting trade history from endpoint",
            exchange_name=self._exchange_name,
            endpoint_path=endpoint_path,
            params=params,
        )

        raw_data, status_code, _ = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params.model_dump(by_alias=True, exclude_none=True),
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

        logger.debug(
            "raw_trade_history_response: Raw trade history response received",
            exchange_name=self._exchange_name,
            raw_data=raw_data,
            status_code=status_code,
        )

        # Use centralized validation
        validated_data = ensure_list_response(raw_data, "trade history", status_code)

        return validated_data, status_code

    def _process_trade_history_response(
        self,
        raw_data: ParsedJsonResponse,
        args: GetTradeHistoryArgs,
        status_code: int,
    ) -> list[Trade]:
        """Process and transform trade history response.

        Since we're using /wapi/v1/history/fills endpoint, we get BackpackRawFill format.
        """
        # Use fills handler since we're calling /wapi/v1/history/fills
        raw_fills_list = self._response_handler.handle_get_fills_response(
            raw_data,
            args.symbol,
            status_code,
        )

        internal_trades: list[Trade] = []
        for raw_fill_model in raw_fills_list:
            try:
                trade = self._mapper.transform_raw_fill_to_internal(raw_fill_model)
                if trade is not None:  # Mapper can return None
                    internal_trades.append(trade)
            except (ValidationError, ValueError) as e_map_item:
                logger.warning(
                    "skipping_fill_mapping: Skipping fill mapping due to error",
                    exchange_name=self._exchange_name,
                    order_id=raw_fill_model.order_id,
                    error=str(e_map_item),
                    raw_fill=raw_fill_model.model_dump_json(exclude_none=True),
                )

        logger.debug(
            "trades_mapped_from_fills: Mapped internal trades from fills",
            exchange_name=self._exchange_name,
            trade_count=len(internal_trades),
        )
        return internal_trades

    def _handle_trade_history_exceptions(
        self,
        e: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle various trade history-related exceptions."""
        if isinstance(e, APIError):
            raise
        if isinstance(e, TransformationError):
            logger.exception(
                "transform_failed: Failed to transform exchange data for trade history",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        if isinstance(e, ValidationError):
            logger.exception(
                "validation_failed: Internal data validation failed for trade history",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        if isinstance(e, ValueError | TypeError):
            error_msg = str(e)
            if current_method in error_msg and "limit" in error_msg:
                raise
            logger.exception(
                "service_logic_error: Service internal logic error for trade history",
                exchange_name=self._exchange_name,
                method=current_method,
                error=str(e),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        logger.exception(
            "unexpected_service_failure: Unexpected service failure for trade history",
            exchange_name=self._exchange_name,
            method=current_method,
            error=str(e),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Retrieves historical trade data (fills).

        Args:
            args: Parameters for filtering trade history including symbol and limit.

        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_trade_history"

        raw_response_content: str | None = None
        status_code: int = 0

        try:
            raw_data, status_code = await self._execute_trade_history_request(args)
            raw_response_content = str(raw_data)
            return self._process_trade_history_response(raw_data, args, status_code)
        except Exception as e:
            self._handle_trade_history_exceptions(
                e,
                current_method,
                status_code,
                raw_response_content,
            )
            # DEFENSIVE CHECK: This should never be reached as
            # _handle_trade_history_exceptions raises
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in get_trade_history",
                original_exception=e,
                http_status=status_code,
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
            "starting_account_settings_update: Starting account settings update",
            class_name=self.__class__.__name__,
            method=current_method,
        )

        try:
            # Build payload using request builder
            payload = self._request_builder.build_update_account_settings_payload(
                auto_borrow_settlements=args.auto_borrow_settlements,
                auto_lend=args.auto_lend,
                auto_realize_pnl=args.auto_realize_pnl,
                auto_repay_borrows=args.auto_repay_borrows,
                leverage_limit=args.leverage_limit,
            )

            endpoint_path = "/api/v1/account"
            logger.debug(
                "updating_account_settings: Updating account settings at endpoint",
                exchange_name=self._exchange_name,
                endpoint_path=endpoint_path,
                payload=payload,
            )

            # Execute PATCH request
            _, status_code, _ = await self._http_client_requester(
                method="PATCH",
                endpoint=endpoint_path,
                data=payload,
                instruction="accountUpdate",
            )

            logger.debug(
                "account_settings_updated: Account settings updated successfully",
                class_name=self.__class__.__name__,
                method=current_method,
                status_code=status_code,
            )

            # Transform updated settings to internal model
            return self._mapper.transform_account_settings_update_to_internal(
                args,
                self._exchange_name,
            )

        except Exception as e:
            logger.error(
                "account_settings_update_failed: Failed to update account settings",
                class_name=self.__class__.__name__,
                method=current_method,
                error=str(e),
            )
            if isinstance(e, APIError):
                raise
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in update_account_settings",
                original_exception=e,
            ) from e
