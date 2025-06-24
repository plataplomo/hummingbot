"""CyberDeltaEngine: Hyperliquid Account Service.

----------------------------------------------

This service encapsulates the logic for fetching and managing account-specific
information from the Hyperliquid Exchange. It uses relevant HttpClient (via requester),
HyperliquidRequestBuilder, HyperliquidResponseHandler, and Mappers to interact
with the API and returns Internal Domain Models.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, NoReturn

from pydantic import ValidationError

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse

# Mappers
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)
from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper

# Import for queryOrderHistory: HyperliquidRawHistoricalOrder for mapper,
# HyperliquidRawHistoricalOrderResponse for handler return type
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
    HyperliquidRawHistoricalOrderResponse,
)

# Imports for open orders
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,  # Type for raw_order.trigger
    HyperliquidRawSimpleOpenOrder,
)

# Import modules for user fills handling
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsResponse,
)

# Import HyperliquidRawClearinghouseState
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    GetOpenOrdersArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    GetUserFillsArgs,
    GetUserStateArgs,
    TransferArgs,
    UpdateAccountSettingsArgs,
    UpdateLeverageArgs,
    WithdrawArgs,
)
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.logging_config import get_logger

# Core Domain Models
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    Order,  # For order history
    SpotBalance,
    Trade,  # For trade history
)
from cyberdelta.core.models.account_settings import AccountSettings
from cyberdelta.core.models.operations import Transfer, Withdrawal  # If HL supports these

if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidAccountService:
    """Service class for Hyperliquid account management operations.

    Returns Internal Domain Models.
    """

    _http_client_requester: HttpClientRequesterSig
    _request_builder: HyperliquidRequestBuilder
    _response_handler: HyperliquidResponseHandler
    _account_mapper: HyperliquidAccountDataMapper  # For account data mappings
    _trading_mapper: HyperliquidTradingDataMapper  # For trading data mappings
    _authenticator: IAuthenticator | None
    _exchange_name: str
    _wallet_address: str | None
    _get_asset_index_callable: Callable[[str], Awaitable[int]]

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        wallet_address: str | None,
        # Add mapper dependencies
        account_mapper: HyperliquidAccountDataMapper,
        trading_mapper: HyperliquidTradingDataMapper,
        get_asset_index_callable: Callable[[str], Awaitable[int]],
    ) -> None:
        """Initialize the Hyperliquid account service with required dependencies.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            wallet_address: Wallet address for authenticated operations (optional)
            account_mapper: Mapper for converting raw account data to internal models
            trading_mapper: Mapper for converting raw trading data to internal models
            get_asset_index_callable: Function to retrieve asset index for symbols

        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address
        # Assign injected mappers
        self._account_mapper = account_mapper
        self._trading_mapper = trading_mapper
        self._get_asset_index_callable = get_asset_index_callable

    async def _get_raw_clearinghouse_state(self) -> HyperliquidRawClearinghouseState:
        """Fetch and validate the raw HyperliquidClearinghouseState."""
        if not self._wallet_address:
            logger.error(
                f"[{self._exchange_name}] Wallet address not set. "
                f"Cannot fetch clearinghouse state.",
            )
            raise APIError(
                message="Wallet address is required to fetch clearinghouse state for Hyperliquid.",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        endpoint_path = "/info"
        payload_model = self._request_builder.build_user_state_payload(
            GetUserStateArgs(wallet_address=self._wallet_address)
        )
        payload_dict = payload_model.model_dump()

        logger.debug(
            f"[{self._exchange_name}] Requesting user state for clearinghouse_state from "
            f"{endpoint_path} with payload: {payload_dict}",
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=payload_dict,
                is_signed=False,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw user state response for clearinghouse_state: "
                f"{raw_data!r} (Status: {status_code})",
            )

            # Handle both list and dict responses for clearinghouse state
            # First try as dict (single user state)
            try:
                validated_dict = ensure_dict_response(
                    raw_data, "user state (clearinghouse)", status_code
                )
                # Convert dict response to list format expected by handler
                raw_data = [validated_dict]
            except APIError:
                # If not a dict, must be a list
                validated_list = ensure_list_response(
                    raw_data, "user state (clearinghouse)", status_code
                )
                if not validated_list:
                    raise APIError(
                        message="Empty user state response for clearinghouse_state",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                        http_status=status_code,
                    ) from None
                raw_data = validated_list

            # The response handler will validate the structure, but we need to ensure
            # we're passing the right part of the response. Hyperliquid returns user state
            # as a list with one dict element.
            if len(raw_data) == 0:
                raise APIError(
                    message="Empty list received for user state response",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # Pass the first element directly to the handler
            # The handler will validate it's a dict and has the correct structure
            return self._response_handler.handle_info_user_state_response(
                raw_response_content=raw_data[0],
                user_address=self._wallet_address,
                status_code=status_code,
            )
        except APIError:  # Re-raise APIErrors directly
            raise
        except (ValidationError, ValueError) as e_val:  # Catch Pydantic/parsing errors
            logger.error(
                f"Validation/map error for HL clearinghouse_state: {e_val}. "
                f"Raw: {raw_data!r}, Status: {status_code}",
            )
            raise APIError(
                message=f"Processing HL clearinghouse_state data failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled:  # Catch any other unexpected errors
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for HL clearinghouse_state: {e_unhandled}. "
                f"{raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for HL clearinghouse_state: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Retrieve all account balances (spot balances derived from user state)."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_balances"

        # No input parameters to validate for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            raw_clearinghouse_state = await self._get_raw_clearinghouse_state()
            internal_balances = (
                self._account_mapper.transform_raw_clearinghouse_state_to_spot_balances(
                    raw_clearinghouse_state,
                )
            )
            logger.debug(f"[{self._exchange_name}] Mapped internal balances: {internal_balances}")
            return internal_balances

        except APIError:
            # Re-raise APIErrors from _get_raw_clearinghouse_state, ResponseHandler, etc.
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
            # Distinguish input validation from internal errors per ERROR_HANDLING.md
            # No input parameters to validate in get_balances, so wrap as internal error
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
        """Retrieve derivative positions, optionally filtered by symbol."""
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
            raw_clearinghouse_state = await self._get_raw_clearinghouse_state()
            # Assuming mapper returns Dict[str, DerivativePosition] where key is symbol
            all_positions_dict = (
                self._account_mapper.transform_raw_clearinghouse_state_to_derivative_positions(
                    raw_clearinghouse_state,
                )
            )

            if symbol:
                position = all_positions_dict.get(symbol)
                if position:
                    logger.debug(
                        f"[{self._exchange_name}] Filtered position for symbol "
                        f"'{symbol}': {position}",
                    )
                    return [position]
                logger.debug(
                    f"[{self._exchange_name}] No position found for symbol '{symbol}'. "
                    f"Positions: {list(all_positions_dict.keys())}",
                )
                return []

            all_positions_list = list(all_positions_dict.values())
            logger.debug(
                f"[{self._exchange_name}] Mapped all internal positions: {all_positions_list}",
            )
            return all_positions_list

        except APIError:
            # Re-raise APIErrors from _get_raw_clearinghouse_state, ResponseHandler, etc.
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
            # Distinguish input validation from internal errors per ERROR_HANDLING.md
            error_msg = str(e_service_logic)
            if current_method in error_msg and "symbol" in error_msg:
                # Re-raise input validation errors
                raise
            else:
                # Wrap internal errors as APIError
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

    async def get_account_summary(self) -> MarginAccountSummary:
        """Retrieve general account information or summary from the clearinghouse state."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_account_summary"

        # No input parameters to validate for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            raw_clearinghouse_state = await self._get_raw_clearinghouse_state()
            internal_summary = (
                self._account_mapper.transform_raw_clearinghouse_state_to_margin_summary(
                    raw_clearinghouse_state,
                )
            )
            logger.debug(
                f"[{self._exchange_name}] Mapped internal account summary: {internal_summary}",
            )
            return internal_summary

        except APIError:
            # Re-raise APIErrors from _get_raw_clearinghouse_state, ResponseHandler, etc.
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

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Retrieve historical order data using the 'historicalOrders' endpoint."""
        self._validate_order_history_args(args)

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            raw_data, status_code, raw_response_content = await self._fetch_order_history_data(args)
            # Validation already done in _fetch_order_history_data
            if raw_data is None:
                return []
            raw_historical_orders = self._process_order_history_response(raw_data)
            internal_orders = self._map_historical_orders_to_internal(raw_historical_orders)

            # Filter by time range since historicalOrders endpoint doesn't support time filtering
            if args.start_time and args.end_time:
                internal_orders = [
                    order
                    for order in internal_orders
                    if order.created_at and args.start_time <= order.created_at <= args.end_time
                ]

            return self._filter_orders_by_symbol(internal_orders, args.symbol)

        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_transformation_error(
                e_transform, "get_order_history", status_code, raw_response_content
            )
        except ValidationError as e_val:
            self._handle_validation_error(
                e_val, "get_order_history", status_code, raw_response_content
            )
        except (ValueError, TypeError) as e_service_logic:
            self._handle_service_logic_error(e_service_logic, "get_order_history")
        except Exception as e_unexpected:
            self._handle_unexpected_error(
                e_unexpected, "get_order_history", status_code, raw_response_content
            )

        # DEFENSIVE CHECK: This should never be reached as all error handlers raise exceptions
        raise AssertionError("Unreachable code: all error handlers should raise exceptions")

    def _validate_order_history_args(self, args: GetOrderHistoryArgs) -> None:
        """Validate order history arguments."""
        if not args.start_time:
            raise ValueError("'start_time' is required for Hyperliquid.")
        if not args.end_time:
            raise ValueError("'end_time' is required for Hyperliquid.")

    async def _fetch_order_history_data(
        self, args: GetOrderHistoryArgs
    ) -> tuple[ParsedJsonResponse | None, int, str | None]:
        """Fetch raw order history data from API using historicalOrders endpoint."""
        if not self._wallet_address:
            raise APIError(
                message="Wallet address is required to fetch order history for Hyperliquid.",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        endpoint_path = "/info"

        # Use the historicalOrders endpoint which doesn't accept time parameters
        payload_model = self._request_builder.build_historical_orders_payload(
            wallet_address=self._wallet_address,
        )
        payload_dict = payload_model.model_dump(by_alias=True, exclude_none=True)

        logger.debug(
            f"[{self._exchange_name}] Requesting order history from {endpoint_path} "
            f"with payload: {payload_dict}",
        )

        raw_data, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload_dict,
            is_signed=False,
        )

        raw_response_content = str(raw_data) if raw_data is not None else None

        logger.debug(
            f"[{self._exchange_name}] Raw order history response: {raw_data!r} "
            f"(Status: {status_code})",
        )

        # Use centralized validation
        validated_data = ensure_list_response(raw_data, "order history", status_code)
        raw_data = validated_data

        return raw_data, status_code, raw_response_content

    def _process_order_history_response(
        self, raw_data: ParsedJsonResponse
    ) -> list[HyperliquidRawHistoricalOrder]:
        """Process the raw response and extract historical orders."""
        if self._wallet_address is None:
            raise APIError(
                message="Wallet address is required for order history processing",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        raw_historical_order_responses: list[HyperliquidRawHistoricalOrderResponse] = (
            self._response_handler.handle_historical_orders_response(
                raw_response_content=raw_data,
                user_address=self._wallet_address,
            )
        )

        # Create combined order models with status for the mapper
        combined_orders: list[HyperliquidRawHistoricalOrder] = []
        for resp in raw_historical_order_responses:
            # Combine order data with status fields
            combined_data = {
                **resp.order.model_dump(by_alias=True),
                "status": resp.status,
                "statusTimestamp": resp.status_timestamp,
            }
            combined_orders.append(HyperliquidRawHistoricalOrder.model_validate(combined_data))

        return combined_orders

    def _map_historical_orders_to_internal(
        self, raw_orders: list[HyperliquidRawHistoricalOrder]
    ) -> list[Order]:
        """Map raw historical orders to internal Order objects."""
        internal_orders: list[Order] = []

        for raw_hist_order in raw_orders:
            try:
                mapped_order = self._trading_mapper.transform_raw_historical_order_to_internal(
                    raw_historical_order=raw_hist_order,
                    trigger=None,
                )
                if mapped_order:
                    internal_orders.append(mapped_order)
            except (ValidationError, ValueError) as e_map_item:
                raw_order_repr = (
                    raw_hist_order.model_dump_json()
                    if hasattr(raw_hist_order, "model_dump_json")
                    else str(raw_hist_order)
                )
                logger.warning(
                    f"[{self._exchange_name}] Error mapping historical order item: "
                    f"{e_map_item}. Raw: {raw_order_repr}",
                )

        return internal_orders

    def _filter_orders_by_symbol(self, orders: list[Order], symbol: str | None) -> list[Order]:
        """Filter orders by symbol if provided."""
        if not symbol:
            logger.debug(
                f"[{self._exchange_name}] Mapped internal order history: {len(orders)} orders",
            )
            return orders

        filtered_orders = [o for o in orders if o.symbol == symbol]
        logger.debug(
            f"[{self._exchange_name}] Filtered order history for symbol '{symbol}': "
            f"{len(filtered_orders)} orders",
        )
        return filtered_orders

    def _handle_transformation_error(
        self,
        error: TransformationError,
        method_name: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> NoReturn:
        """Handle transformation errors consistently."""
        logger.error(
            f"[{self._exchange_name}] {method_name}: Failed to transform exchange data: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_validation_error(
        self,
        error: ValidationError,
        method_name: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> NoReturn:
        """Handle validation errors consistently."""
        logger.error(
            f"[{self._exchange_name}] {method_name}: Internal data validation failed: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_service_logic_error(
        self,
        error: ValueError | TypeError,
        method_name: str,
    ) -> None:
        """Handle service logic errors consistently."""
        logger.error(
            f"[{self._exchange_name}] {method_name}: Service internal logic error: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_unexpected_error(
        self,
        error: Exception,
        method_name: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle unexpected errors consistently."""
        logger.error(
            f"[{self._exchange_name}] {method_name}: Unexpected service failure: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    async def _fetch_trade_history_data(self) -> tuple[ParsedJsonResponse | None, int, str | None]:
        """Fetch raw trade history data from API."""
        if not self._wallet_address:
            raise APIError(
                message="Wallet address is required to fetch trade history for Hyperliquid.",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        endpoint_path = "/info"
        payload_model = self._request_builder.build_user_fills_request_payload(
            GetUserFillsArgs(wallet_address=self._wallet_address)
        )
        payload_dict = payload_model.model_dump()

        logger.debug(
            f"[{self._exchange_name}] Requesting user fills from {endpoint_path} "
            f"with payload: {payload_dict}",
        )

        raw_response_list, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload_dict,
            is_signed=False,
        )

        raw_response_content = str(raw_response_list) if raw_response_list is not None else None

        logger.debug(
            f"[{self._exchange_name}] Raw user fills response: {raw_response_list!r} "
            f"(Status: {status_code})",
        )

        if raw_response_list is None:
            raise APIError(
                message=f"No data received for user fills, status: {status_code}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        return raw_response_list, status_code, raw_response_content

    def _process_trade_history_response(
        self, raw_response_list: ParsedJsonResponse, status_code: int
    ) -> HyperliquidRawUserFillsResponse:
        """Process the raw response and validate trade history data."""
        # Use centralized validation
        validated_response = ensure_list_response(raw_response_list, "user fills", status_code)

        # The handler expects ParsedJsonResponse which is compatible with our validated_response
        if self._wallet_address is None:
            raise APIError(
                message="Wallet address is required for user fills processing",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        return self._response_handler.handle_info_user_fills_response(
            raw_response_content=validated_response,
            user_address=self._wallet_address,
            status_code=status_code,
        )

    def _map_fills_to_internal_trades(
        self, validated_fills_response: HyperliquidRawUserFillsResponse, symbol_filter: str | None
    ) -> list[Trade]:
        """Map validated fills to internal Trade objects with optional symbol filtering."""
        internal_trades: list[Trade] = []

        # Ensure validated_fills_response.root is not None before iterating
        if validated_fills_response and validated_fills_response.root:
            for raw_fill_obj in validated_fills_response.root:
                try:
                    # Use the injected account_mapper instance
                    mapped_trade = self._account_mapper.transform_raw_user_fill_to_internal(
                        raw_fill_obj,
                    )
                    # Apply symbol filtering if specified
                    if symbol_filter is None or mapped_trade.symbol == symbol_filter:
                        internal_trades.append(mapped_trade)
                except (ValidationError, ValueError) as e_map:
                    logger.warning(
                        f"[{self._exchange_name}] Error mapping raw user fill: {e_map}. "
                        f"Raw: {raw_fill_obj}. Skipping.",
                    )

        return internal_trades

    def _apply_trade_limit(self, trades: list[Trade], limit: int | None) -> list[Trade]:
        """Apply limit to trades if specified."""
        if limit is not None and len(trades) > limit:
            trades = trades[:limit]

        logger.debug(
            f"[{self._exchange_name}] Mapped internal trades: {len(trades)} trades",
        )
        return trades

    async def _fetch_open_orders_data(self) -> tuple[ParsedJsonResponse | None, int, str | None]:
        """Fetch raw open orders data from API."""
        if not self._wallet_address:
            raise APIError(
                message="Wallet address is required to fetch open orders for Hyperliquid.",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        endpoint_path = "/info"  # Hyperliquid uses /info for many user-specific queries
        # Use the request builder to create the payload
        payload_model = self._request_builder.build_open_orders_payload(
            GetOpenOrdersArgs(wallet_address=self._wallet_address)
        )
        payload_dict = payload_model.model_dump()

        logger.debug(
            f"[{self._exchange_name}] Requesting open orders from {endpoint_path} "
            f"with payload: {payload_dict}",
        )

        raw_data, status_code, _ = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload_dict,
            is_signed=False,
        )

        raw_response_content = str(raw_data) if raw_data is not None else None

        logger.debug(
            f"[{self._exchange_name}] Raw open orders response: {raw_data!r} "
            f"(Status: {status_code})",
        )

        # Use centralized validation
        validated_data = ensure_list_response(raw_data, "open orders", status_code)
        raw_data = validated_data

        return raw_data, status_code, raw_response_content

    def _process_open_orders_response(
        self, raw_data: ParsedJsonResponse, status_code: int
    ) -> HyperliquidRawOpenOrdersResponse:
        """Process the raw response and validate open orders data."""
        if self._wallet_address is None:
            raise APIError(
                message="Wallet address is required for open orders processing",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        return self._response_handler.handle_info_open_orders_response(
            raw_response_content=raw_data,
            user_address=self._wallet_address,
            status_code=status_code,
        )

    def _map_open_orders_to_internal(
        self, validated_response: HyperliquidRawOpenOrdersResponse
    ) -> list[Order]:
        """Map validated open orders to internal Order objects."""
        internal_orders: list[Order] = []
        raw_orders_list: list[HyperliquidRawSimpleOpenOrder] = []

        if validated_response and validated_response.items:
            raw_orders_list = validated_response.items

        for raw_order in raw_orders_list:  # raw_order is HyperliquidRawSimpleOpenOrder
            # Use transform_raw_simple_order_to_internal for simple order format
            internal_order = self._trading_mapper.transform_raw_simple_open_order_to_internal(
                raw_simple_order=raw_order
            )
            internal_orders.append(internal_order)

        return internal_orders

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Retrieve user trade history (fills).

        Args:
            args: Parameters for filtering trade history including symbol and limit.

        Note:
            Hyperliquid's userFills endpoint doesn't support server-side filtering.
            Symbol filtering and limit are applied client-side after fetching all fills.

        """
        # Initialize context for error handling
        raw_response_list: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            (
                raw_response_list,
                status_code,
                raw_response_content,
            ) = await self._fetch_trade_history_data()

            if raw_response_list is None:
                raise APIError(
                    message="No data received for trade history",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            validated_fills_response = self._process_trade_history_response(
                raw_response_list, status_code
            )
            internal_trades = self._map_fills_to_internal_trades(
                validated_fills_response, args.symbol
            )
            return self._apply_trade_limit(internal_trades, args.limit)

        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_transformation_error(
                e_transform, "get_trade_history", status_code, raw_response_content
            )
        except ValidationError as e_val:
            self._handle_validation_error(
                e_val, "get_trade_history", status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except (ValueError, TypeError) as e_service_logic:
            self._handle_service_logic_error(e_service_logic, "get_trade_history")
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except Exception as e_unexpected:
            self._handle_unexpected_error(
                e_unexpected, "get_trade_history", status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]

    async def transfer(
        self,
        args: TransferArgs,
    ) -> Transfer:
        """Perform an internal transfer. Details depend on HL capabilities."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "transfer"

        # Args model validation has already been performed
        # Extract parameters from args object (for future implementation)
        _asset = args.asset
        _amount = args.amount
        _from_account = args.from_account_type
        _to_account = args.to_account_type

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.warning(
                f"[{self._exchange_name}] transfer functionality may be limited or "
                f"different for Hyperliquid.",
            )
            raise NotImplementedError("transfer not yet implemented in HyperliquidAccountService")

        except APIError:
            # Re-raise APIErrors from any future implementation
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
        args: WithdrawArgs,
    ) -> Withdrawal:
        """Initiate a withdrawal of funds. Details depend on HL (L1 interaction)."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "withdraw"

        # Args model validation has already been performed
        # Extract parameters from args object (for future implementation)
        _asset = args.asset
        _amount = args.amount
        _destination_address = args.address

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.warning(
                f"[{self._exchange_name}] withdraw functionality is complex for Hyperliquid "
                f"(L1 interaction).",
            )
            raise NotImplementedError("withdraw not yet implemented in HyperliquidAccountService")

        except APIError:
            # Re-raise APIErrors from any future implementation
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for withdraw: {e_transform}",
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
                f"failed for withdraw: {e_val}",
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
            # This is from service internal logic - wrap as APIError
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for withdraw: {e_service_logic}",
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
                f"for withdraw: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_open_orders(self) -> list[Order]:
        """Retrieve all open orders for the account."""
        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            raw_data, status_code, raw_response_content = await self._fetch_open_orders_data()
            # Validation already done in _fetch_open_orders_data
            if raw_data is None:
                return []
            validated_response = self._process_open_orders_response(raw_data, status_code)
            internal_orders = self._map_open_orders_to_internal(validated_response)

            logger.debug(
                f"[{self._exchange_name}] Mapped {len(internal_orders)} internal open orders.",
            )
            return internal_orders

        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_transformation_error(
                e_transform, "get_open_orders", status_code, raw_response_content
            )
        except ValidationError as e_val:
            self._handle_validation_error(
                e_val, "get_open_orders", status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except (ValueError, TypeError) as e_service_logic:
            self._handle_service_logic_error(e_service_logic, "get_open_orders")
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except Exception as e_unexpected:
            self._handle_unexpected_error(
                e_unexpected, "get_open_orders", status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]

    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Update account settings such as leverage limits.

        Args:
            args: Account settings to update

        Returns:
            AccountSettings: Updated account settings with current timestamp

        Note:
            Hyperliquid only supports updating leverage limits, which is set per-asset.
            Other settings like auto_lend are not supported.

        Raises:
            APIError: If leverage_limit is not provided or update fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "update_account_settings"

        logger.debug(
            f"[{self.__class__.__name__}::{current_method}] Starting account settings update"
        )

        # Validate leverage_limit is provided since it's the only setting we can update
        if args.leverage_limit is None:
            raise APIError(
                message="leverage_limit is required for Hyperliquid account settings update. "
                "Other settings (auto_lend, etc.) are not supported by Hyperliquid.",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        # Initialize error handling context
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Convert Decimal to int for leverage
            leverage_int = int(args.leverage_limit)
            if leverage_int < 1 or leverage_int > 100:
                raise APIError(
                    message=f"Invalid leverage value: {leverage_int}. Must be between 1 and 100.",
                    code=APIErrorCode.INVALID_REQUEST.value,
                )

            # Get current positions to update leverage for all active assets
            positions: list[DerivativePosition] = []
            try:
                positions = await self.get_positions()
            except APIError as e:
                logger.warning(
                    f"[{self._exchange_name}] Could not fetch positions for leverage update: {e}"
                )
                # Continue without positions - will return settings without updating

            # Track which assets we updated
            asset_leverage_settings: dict[int, int] = {}

            # Update leverage for each position's asset
            for position in positions:
                if position.symbol and position.size > 0:
                    try:
                        # Get asset index for the symbol
                        asset_index = await self._get_asset_index_callable(position.symbol)

                        # Build the update leverage request
                        request_payload = self._request_builder.build_update_leverage_request(
                            UpdateLeverageArgs(
                                asset_index=asset_index,
                                leverage=leverage_int,
                                is_cross=True,  # Default to cross margin
                            )
                        )

                        # Execute the leverage update via /exchange endpoint
                        _, status_code, _ = await self._http_client_requester(
                            method="POST",
                            endpoint="/exchange",
                            data=request_payload,
                            is_signed=True,
                            endpoint_group="exchange",
                            request_weight=1,
                        )

                        # Track successful updates
                        asset_leverage_settings[asset_index] = leverage_int

                        logger.info(
                            f"[{self._exchange_name}] Updated leverage for {position.symbol} "
                            f"(asset index {asset_index}) to {leverage_int}x"
                        )

                    except Exception as e:
                        logger.warning(
                            f"[{self._exchange_name}] Failed to update leverage for "
                            f"{position.symbol}: {e}"
                        )
                        # Continue with other positions even if one fails

            logger.debug(
                f"[{self.__class__.__name__}::{current_method}] "
                f"Account settings updated successfully"
            )

            # Transform to internal model using mapper
            return self._account_mapper.transform_account_settings_update_to_internal(
                args=args,
                exchange_name=self._exchange_name,
                asset_leverage_settings=(
                    asset_leverage_settings if asset_leverage_settings else None
                ),
            )

        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.__class__.__name__}::{current_method}] "
                f"Failed to update account settings: {e}"
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in update_account_settings",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
