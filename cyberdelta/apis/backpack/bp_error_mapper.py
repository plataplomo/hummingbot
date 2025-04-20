class BackpackErrorMapper:
    """
    Maps and normalizes Backpack API errors to CyberDeltaEngine's canonical error model.

    Responsibilities:
    - Map Backpack error codes (from BackpackRawApiError) to APIErrorCode.
    - Validate and normalize all error data using APIErrorResponse.
    - Raise APIError with all validated fields for unified error propagation.
    - Log ambiguous/unmapped codes for diagnostics and future mapping improvements.

    Usage:
        raise BackpackErrorMapper.map_error_response(...)
    """

    @staticmethod
    def map_error_code(
        error_body: str, error_data: dict[str, Any] | None = None, status_code: int | None = None
    ) -> APIErrorCode:
        """
        Map Backpack error responses (body, data, status) to standardized APIErrorCode.

        - Uses BackpackRawApiError for strict code extraction if possible.
        - Falls back to heuristics if parsing fails.
        - Logs unmapped/ambiguous codes for diagnostics.

        Args:
            error_body: Raw error body as string (for fallback/logging).
            error_data: Parsed error data as dict (if available).
            status_code: HTTP status code (if available).

        Returns:
            APIErrorCode: Canonical error code for internal handling.
        """
        mapped_code = APIErrorCode.EXCHANGE_SPECIFIC
        if error_data:
            try:
                raw_api_error = BackpackRawApiError.model_validate(error_data)
                code = raw_api_error.code.upper()
                code_map = {
                    "INVALID_SIGNATURE": APIErrorCode.AUTHENTICATION_FAILED,
                    "UNAUTHORIZED": APIErrorCode.AUTHENTICATION_FAILED,
                    "FORBIDDEN": APIErrorCode.AUTHENTICATION_FAILED,
                    "TOO_MANY_REQUESTS": APIErrorCode.RATE_LIMITED,
                    "RATE_LIMIT_EXCEEDED": APIErrorCode.RATE_LIMITED,
                    "INVALID_SYMBOL": APIErrorCode.INVALID_SYMBOL,
                    "INVALID_QUANTITY": APIErrorCode.INVALID_ORDER_SIZE,
                    "INVALID_ORDER": APIErrorCode.INVALID_REQUEST,
                    "INVALID_PRICE": APIErrorCode.INVALID_REQUEST,
                    "INVALID_CLIENT_REQUEST": APIErrorCode.INVALID_REQUEST,
                    "INSUFFICIENT_FUNDS": APIErrorCode.INSUFFICIENT_FUNDS,
                    "INSUFFICIENT_MARGIN": APIErrorCode.INSUFFICIENT_FUNDS,
                    "ORDER_LIMIT": APIErrorCode.ORDER_REJECTED,
                    "POSITION_LIMIT": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "SERVER_ERROR": APIErrorCode.SERVER_ERROR,
                    "MAINTENANCE": APIErrorCode.MAINTENANCE,
                    "RESOURCE_NOT_FOUND": APIErrorCode.ORDER_NOT_FOUND,
                    "INVALID_MARKET": APIErrorCode.INVALID_SYMBOL,
                    "INVALID_SOURCE": APIErrorCode.EXCHANGE_SPECIFIC,
                    "ACCOUNT_LIQUIDATING": APIErrorCode.EXCHANGE_SPECIFIC,
                    "TRADING_PAUSED": APIErrorCode.MARKET_CLOSED,
                    "INVALID_ASSET": APIErrorCode.INVALID_SYMBOL,
                    "INVALID_POSITION_ID": APIErrorCode.ORDER_NOT_FOUND,
                    "BORROW_REQUIRES_LEND_REDEEM": APIErrorCode.EXCHANGE_SPECIFIC,
                    "LEND_REQUIRES_BORROW_REPAY": APIErrorCode.EXCHANGE_SPECIFIC,
                    "INSUFFICIENT_SUPPLY": APIErrorCode.INSUFFICIENT_FUNDS,
                    "BORROW_LIMIT": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "LEND_LIMIT": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "MAX_LEVERAGE_REACHED": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "PRECONDITION_FAILED": APIErrorCode.INVALID_REQUEST,
                    "NOT_IMPLEMENTED": APIErrorCode.EXCHANGE_SPECIFIC,
                }
                mapped_code = code_map.get(code, APIErrorCode.EXCHANGE_SPECIFIC)
                if mapped_code == APIErrorCode.EXCHANGE_SPECIFIC:
                    logger.warning(
                        f"[BackpackErrorMapper] Unmapped or ambiguous Backpack error code: {code}"
                    )
                return mapped_code
            except Exception as e:
                logger.warning(
                    f"[BackpackErrorMapper] Failed to parse error_data as "
                    f"BackpackRawApiError: {e}. Falling back to heuristics."
                )
        return mapped_code

    @staticmethod
    def map_error_response(
        status_code: int | None,
        error_body: str,
        error_data: dict[str, Any] | None = None,
        request_path: str | None = None,
        exchange_message: str | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """
        Map Backpack error responses to a standardized APIError, including error code
        mapping and full validation.

        - Always validates and normalizes error data using APIErrorResponse.
        - Ensures all error propagation is type-safe and consistent.
        - Handles edge cases: unmapped codes, malformed payloads, chained exceptions.

        Args:
            status_code: HTTP status code (if available).
            error_body: Raw error body as string.
            error_data: Parsed error data as dict (if available).
            request_path: API endpoint path (for diagnostics).
            exchange_message: Exchange-provided error message (if available).
            original_exception: Chained exception (if any).

        Returns:
            APIError: Exception ready to be raised or propagated.
        """
        mapped_code = BackpackErrorMapper.map_error_code(
            error_body=error_body, error_data=error_data, status_code=status_code
        )
        # Guarantee message is always a str
        msg: str = error_body
        if exchange_message is not None:
            msg = exchange_message
        elif error_data and isinstance(error_data.get("msg"), str):
            msg = error_data["msg"]
        api_error_response = APIErrorResponse.from_exchange_error(
            message=msg,
            code=mapped_code.value,
            http_status=status_code,
            exchange_code=(
                str(error_data.get("code")) if error_data and "code" in error_data else None
            ),
            exchange_message=(error_data.get("msg") if error_data else None),
            metadata={"request_path": request_path} if request_path else None,
        )
        # Convert APIErrorResponse to APIErrorModel-compatible fields
        # APIErrorModel expects code: APIErrorCode, exchange_code: str|None
        # Defensive: ensure code is valid APIErrorCode, else EXCHANGE_SPECIFIC
        try:
            code_enum = (
                APIErrorCode(api_error_response.code)
                if isinstance(api_error_response.code, int)
                else APIErrorCode.EXCHANGE_SPECIFIC
            )
        except Exception:
            code_enum = APIErrorCode.EXCHANGE_SPECIFIC
        exchange_code_str = (
            str(api_error_response.exchange_code)
            if api_error_response.exchange_code is not None
            else None
        )
        # code_enum is always an APIErrorCode, so use .value directly
        return APIError(
            message=api_error_response.message,
            code=code_enum.value,
            http_status=api_error_response.http_status,
            exchange_code=exchange_code_str,
            exchange_message=api_error_response.exchange_message,
            retry_after=api_error_response.retry_after,
            original_exception=original_exception,
        )
