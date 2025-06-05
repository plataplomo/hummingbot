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

# from typing import TYPE_CHECKING, Any # Any no longer used
from typing import TYPE_CHECKING, Any, cast  # Import cast and Any

from pydantic import ValidationError

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse

# Mappers
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
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
    HyperliquidRawOpenOrder,
    HyperliquidRawOpenOrdersResponse,  # Type for raw_order.trigger
)

# Import HyperliquidRawUserFill (used as type hint for raw_fill in get_trade_history)
# and HyperliquidRawUserFillsResponse (returned by handler in get_trade_history)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFill,
)

# Import HyperliquidRawClearinghouseState
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    TransferArgs,
    WithdrawArgs,
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
        payload_model = self._request_builder.build_user_state_payload(self._wallet_address)
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
                is_signed=True,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw user state response for clearinghouse_state: "
                f"{raw_data!r} (Status: {status_code})",
            )

            if raw_data is None:
                raise APIError(
                    message=(
                        f"No data received for user state (for clearinghouse_state), "
                        f"status: {status_code}"
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            if not isinstance(raw_data, list) or not raw_data:
                raise APIError(
                    message=(
                        f"Unexpected raw user state response format, expected non-empty list, "
                        f"got {type(raw_data)}"
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            state_data_dict = raw_data[0]
            if not isinstance(state_data_dict, dict):
                raise APIError(
                    message=(
                        f"Unexpected item format in user state response, expected dict, "
                        f"got {type(state_data_dict)}"
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            return self._response_handler.handle_info_user_state_response(
                raw_response_content=cast(
                    dict[str, Any],
                    state_data_dict,
                ),  # Cast to dict[str, Any]
                user_address=self._wallet_address,
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

    async def get_account_summary(self) -> MarginAccountSummary | None:
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
        """Retrieve historical order data using the 'queryOrderHistory' endpoint.

        Requires start_time and end_time.
        Filtering by symbol (if provided) is done client-side.
        """
        # Service Input Parameter Validation - Hyperliquid specific requirements
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_history"

        # Hyperliquid requires both start_time and end_time
        if not args.start_time:
            raise ValueError(f"[{current_method}] 'start_time' is required for Hyperliquid.")
        if not args.end_time:
            raise ValueError(f"[{current_method}] 'end_time' is required for Hyperliquid.")

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            if not self._wallet_address:
                raise APIError(
                    message="Wallet address is required to fetch order history for Hyperliquid.",
                    code=APIErrorCode.INVALID_REQUEST.value,
                )

            endpoint_path = "/info"
            start_time_ms = int(args.start_time.timestamp() * 1000)
            end_time_ms = int(args.end_time.timestamp() * 1000)

            payload_model = self._request_builder.build_order_history_payload(
                wallet_address=self._wallet_address,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            payload_dict = payload_model.model_dump()

            logger.debug(
                f"[{self._exchange_name}] Requesting order history from {endpoint_path} "
                f"with payload: {payload_dict}",
            )

            raw_data, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=payload_dict,
                is_signed=True,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                f"[{self._exchange_name}] Raw order history response: {raw_data!r} "
                f"(Status: {status_code})",
            )

            if raw_data is None:
                raise APIError(
                    message=f"No data received for order history, status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # handle_query_order_history_response expects the raw list and returns
            # list[HyperliquidRawHistoricalOrderResponse]
            raw_historical_order_responses: list[HyperliquidRawHistoricalOrderResponse] = (
                self._response_handler.handle_query_order_history_response(
                    raw_response_content=raw_data,  # Pass the raw list directly
                    user_address=self._wallet_address,
                )
            )

            # Extract the actual HyperliquidRawHistoricalOrder from each response object
            actual_raw_historical_orders: list[HyperliquidRawHistoricalOrder] = [
                resp.order for resp in raw_historical_order_responses
            ]

            # The mapper method transform_raw_historical_order_to_internal needs to be
            # defined in HyperliquidOrderMapper
            internal_orders: list[Order] = []
            if actual_raw_historical_orders:
                for raw_hist_order in actual_raw_historical_orders:
                    try:
                        mapped_order = (
                            self._trading_mapper.transform_raw_historical_order_to_internal(
                                raw_historical_order=raw_hist_order,
                                trigger=None,  # Assuming no separate trigger info
                                # for historical orders here
                            )
                        )
                        if mapped_order:
                            # Client-side symbol filtering
                            if args.symbol is None or mapped_order.symbol == args.symbol:
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

            if args.symbol:
                # Assuming Order object has an instrument_symbol attribute after mapping
                # which would correspond to raw_hist_order.asset
                filtered_orders = [o for o in internal_orders if o.symbol == args.symbol]
                logger.debug(
                    f"[{self._exchange_name}] Filtered order history for symbol '{args.symbol}': "
                    f"{len(filtered_orders)} orders",
                )
                return filtered_orders

            logger.debug(
                f"[{self._exchange_name}] Mapped internal order history: "
                f"{len(internal_orders)} orders",
            )
            return internal_orders

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
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

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Retrieve user trade history (fills).

        Args:
            args: Parameters for filtering trade history including symbol and limit.

        Note:
            Hyperliquid's userFills endpoint doesn't support server-side filtering.
            Symbol filtering and limit are applied client-side after fetching all fills.

        """
        # Service Input Parameter Validation is now handled by GetTradeHistoryArgs Pydantic model
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_trade_history"

        # Initialize context for error handling
        raw_response_list: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            if not self._wallet_address:
                raise APIError(
                    message="Wallet address is required to fetch trade history for Hyperliquid.",
                    code=APIErrorCode.INVALID_REQUEST.value,
                )

            endpoint_path = "/info"
            payload_model = self._request_builder.build_user_fills_request_payload(
                self._wallet_address,
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
                is_signed=True,
            )

            if raw_response_list is not None:
                raw_response_content = str(raw_response_list)

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

            # Response for userFills is directly a list of fill objects
            # (RawJsonResponse is list[dict[str,Any]])
            # Need to cast to the expected type for the handler
            if not isinstance(raw_response_list, list):
                raise APIError(
                    message=(
                        f"Unexpected user fills response format, expected list, "
                        f"got {type(raw_response_list)}"
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # Cast to list[dict[str,Any]] as RawJsonResponse is list[dict]
            raw_fills_list_of_dicts = cast(list[dict[str, Any]], raw_response_list)

            # The handler expects list[RawJsonResponseItem] which is list[dict[str, Any]]
            # and user_address
            validated_fills_response = self._response_handler.handle_info_user_fills_response(
                raw_response_content=cast(
                    RawJsonResponse,
                    raw_fills_list_of_dicts,
                ),  # Cast to satisfy handler
                user_address=self._wallet_address,  # Add missing user_address
            )

            internal_trades: list[Trade] = []
            raw_fill_obj: HyperliquidRawUserFill
            # Ensure validated_fills_response.root is not None before iterating
            if validated_fills_response and validated_fills_response.root:
                for raw_fill_obj in validated_fills_response.root:
                    try:
                        # Use the injected account_mapper instance
                        mapped_trade = self._account_mapper.transform_raw_user_fill_to_internal(
                            raw_fill_obj,
                        )
                        # Apply symbol filtering if specified
                        if args.symbol is None or mapped_trade.symbol == args.symbol:
                            internal_trades.append(mapped_trade)
                    except (ValidationError, ValueError) as e_map:
                        logger.warning(
                            f"[{self._exchange_name}] Error mapping raw user fill: {e_map}. "
                            f"Raw: {raw_fill_obj}. Skipping.",
                        )
            # Apply limit if specified
            if args.limit is not None and len(internal_trades) > args.limit:
                internal_trades = internal_trades[: args.limit]

            logger.debug(
                f"[{self._exchange_name}] Mapped internal trades: {len(internal_trades)} trades",
            )
            return internal_trades

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
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
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_open_orders"

        # No input parameters to validate for this method

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            if not self._wallet_address:
                raise APIError(
                    message="Wallet address is required to fetch open orders for Hyperliquid.",
                    code=APIErrorCode.INVALID_REQUEST.value,
                )

            endpoint_path = "/info"  # Hyperliquid uses /info for many user-specific queries
            # Use the request builder to create the payload
            payload_model = self._request_builder.build_open_orders_payload(
                wallet_address=self._wallet_address,
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
                is_signed=True,
            )

            if raw_data is not None:
                raw_response_content = str(raw_data)

            logger.debug(
                f"[{self._exchange_name}] Raw open orders response: {raw_data!r} "
                f"(Status: {status_code})",
            )

            if raw_data is None:
                raise APIError(
                    message=f"No data received for open orders, status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # This method would internally handle Pydantic validation for each raw order.
            raw_orders_list: list[HyperliquidRawOpenOrder] = []  # Initialize
            validated_response: HyperliquidRawOpenOrdersResponse = (
                self._response_handler.handle_info_open_orders_response(
                    raw_response_content=raw_data,
                    user_address=self._wallet_address,
                )
            )
            if validated_response and validated_response.items:
                raw_orders_list = validated_response.items

            internal_orders: list[Order] = []
            for raw_order in raw_orders_list:  # raw_order is HyperliquidRawOpenOrder
                # Use transform_raw_order_to_internal, passing .order and .trigger
                internal_order = self._trading_mapper.transform_raw_order_to_internal(
                    raw_order=raw_order.order,  # This is HyperliquidRawOrderData
                    trigger=raw_order.trigger,  # This is HyperliquidRawTriggerData | None
                )
                internal_orders.append(internal_order)

            logger.debug(
                f"[{self._exchange_name}] Mapped {len(internal_orders)} internal open orders.",
            )
            return internal_orders

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
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
