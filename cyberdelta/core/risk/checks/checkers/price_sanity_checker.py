"""Price sanity checker implementation with direct configuration access."""

import statistics
from decimal import Decimal
from typing import Any, Final

from cyberdelta.config import AppSettings
from cyberdelta.core.risk.checks.checkers.typed_base_checker import TypedBaseChecker
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.core.risk.exceptions.check_exceptions import PriceSanityError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Price anomaly detection constants
MIN_HISTORICAL_PRICES_FOR_ANOMALY_DETECTION = 3  # Minimum historical prices for anomaly detection
OUTLIER_Z_SCORE_THRESHOLD = 3.0  # Standard statistical threshold for outliers


class PriceSanityChecker(TypedBaseChecker[CheckResult]):
    """Checker that validates price reasonableness and detects anomalies."""

    CHECKER_NAME: Final[str] = "price_sanity"

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the price sanity checker with direct AppSettings access.

        Args:
            app_settings: The application settings instance
        """
        super().__init__(app_settings, self.CHECKER_NAME)

        # Cache frequently accessed values for performance
        self._min_price = self.thresholds.min_price
        self._max_price = self.thresholds.max_price
        self._max_deviation = self.thresholds.max_price_deviation
        self._max_spread = self.thresholds.max_price_spread
        self._outlier_detection = self.checker_settings.enable_outlier_detection
        self._z_score_threshold = self.thresholds.outlier_z_score_threshold

        # Price precision validation (hardcoded for now as not in config model)
        self.max_decimal_places = 8
        self.enable_precision_check = True

        # Historical price tracking (for anomaly detection)
        self.price_history: dict[str, list[Decimal]] = {}  # symbol -> list of recent prices
        self.max_history_size = 100

        # Market data validation
        self.enable_market_data_check = True
        self.market_data_tolerance = Decimal("0.1")  # 10%

    @property
    def name(self) -> str:
        """Name of the checker."""
        return "price_sanity"

    async def _perform_check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Check that prices are reasonable and detect anomalies.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check

        Returns:
            CheckResult indicating success/failure
        """
        details: dict[str, Any] = {}

        # Get prices
        long_price = getattr(opportunity, "long_price", None)
        short_price = getattr(opportunity, "short_price", None)
        symbol = getattr(opportunity, "symbol", "unknown")

        if long_price is None or short_price is None:
            return CheckResult.failure(
                message="Cannot check price sanity: missing price data",
                details={"long_price": long_price, "short_price": short_price},
            )

        # Convert to Decimal
        try:
            long_price_decimal = self._to_decimal(long_price)
            short_price_decimal = self._to_decimal(short_price)
        except (ValueError, TypeError) as e:
            return CheckResult.failure(
                message=f"Invalid price format: {e!s}",
                details={"long_price": str(long_price), "short_price": str(short_price)},
            )

        details.update({
            "long_price": float(long_price_decimal),
            "short_price": float(short_price_decimal),
            "symbol": symbol,
        })

        # Check price bounds
        price_bounds_result = self._check_price_bounds(long_price_decimal, short_price_decimal)
        if not price_bounds_result.passed:
            details.update(price_bounds_result.details or {})
            return CheckResult.failure(
                message=f"Price bounds check failed: {price_bounds_result.message}",
                details=details,
            )

        # Check price precision
        if self.enable_precision_check:
            precision_result = self._check_price_precision(long_price_decimal, short_price_decimal)
            if not precision_result.passed:
                details.update(precision_result.details or {})
                return CheckResult.failure(
                    message=f"Price precision check failed: {precision_result.message}",
                    details=details,
                )

        # Check spread reasonableness
        spread_result = self._check_spread_reasonableness(long_price_decimal, short_price_decimal)
        if not spread_result.passed:
            details.update(spread_result.details or {})
            return CheckResult.failure(
                message=f"Spread reasonableness check failed: {spread_result.message}",
                details=details,
            )

        # Check for price anomalies
        if self._outlier_detection:
            anomaly_result = self._check_price_anomalies(
                symbol,
                long_price_decimal,
                short_price_decimal,
            )
            if not anomaly_result.passed:
                details.update(anomaly_result.details or {})
                return CheckResult.failure(
                    message=f"Price anomaly detected: {anomaly_result.message}",
                    details=details,
                )

        # Update price history
        self._update_price_history(symbol, long_price_decimal, short_price_decimal)

        # Calculate price sanity score
        avg_price = (long_price_decimal + short_price_decimal) / 2
        spread_percentage = abs(long_price_decimal - short_price_decimal) / avg_price

        details.update({
            "average_price": float(avg_price),
            "spread_percentage": float(spread_percentage),
            "price_bounds_ok": True,
            "precision_ok": True,
            "spread_reasonable": True,
            "no_anomalies": True,
        })

        return CheckResult.success(
            message=(
                f"Price sanity check passed: avg=${avg_price:.6f}, spread={spread_percentage:.4%}"
            ),
            details=details,
        )

    def _to_decimal(self, value: Decimal | str | float) -> Decimal:
        """Convert value to Decimal with validation.

        Args:
            value: The value to convert to Decimal.

        Returns:
            The value as a Decimal.

        Raises:
            PriceSanityError: If the value type cannot be converted.
        """
        if isinstance(value, Decimal):
            return value
        if isinstance(value, str):
            return Decimal(value)
        if isinstance(value, float):
            return Decimal(str(value))
        # This should never be reached due to type constraints
        raise PriceSanityError(
            PriceSanityError.INVALID_TYPE_CONVERSION,
            metadata={"value_type": str(type(value))},
            checker_name="PriceSanityChecker",
            check_type="price_conversion",
        )

    def _check_price_bounds(self, long_price: Decimal, short_price: Decimal) -> CheckResult:
        """Check if prices are within reasonable bounds.

        Args:
            long_price: The long/buy price to check.
            short_price: The short/sell price to check.

        Returns:
            CheckResult indicating whether prices are within bounds.
        """
        for price_name, price in [("long_price", long_price), ("short_price", short_price)]:
            if price <= 0:
                return CheckResult.failure(
                    message=f"{price_name} must be positive: {price}",
                    details={price_name: float(price)},
                )

            if price < self._min_price:
                return CheckResult.failure(
                    message=f"{price_name} {price} below minimum {self._min_price}",
                    details={price_name: float(price), "min_price": float(self._min_price)},
                )

            if price > self._max_price:
                return CheckResult.failure(
                    message=f"{price_name} {price} above maximum {self._max_price}",
                    details={price_name: float(price), "max_price": float(self._max_price)},
                )

        return CheckResult.success("Price bounds check passed")

    def _check_price_precision(self, long_price: Decimal, short_price: Decimal) -> CheckResult:
        """Check if prices have reasonable precision.

        Args:
            long_price: The long/buy price to check.
            short_price: The short/sell price to check.

        Returns:
            CheckResult indicating whether prices have acceptable precision.
        """
        for price_name, price in [("long_price", long_price), ("short_price", short_price)]:
            # Count decimal places
            price_str = str(price)
            if "." in price_str:
                decimal_places = len(price_str.split(".")[1])
                if decimal_places > self.max_decimal_places:
                    return CheckResult.failure(
                        message=(
                            f"{price_name} has too many decimal places: "
                            f"{decimal_places} > {self.max_decimal_places}"
                        ),
                        details={
                            price_name: float(price),
                            "decimal_places": decimal_places,
                            "max_decimal_places": self.max_decimal_places,
                        },
                    )

        return CheckResult.success("Price precision check passed")

    def _check_spread_reasonableness(
        self,
        long_price: Decimal,
        short_price: Decimal,
    ) -> CheckResult:
        """Check if spread is within reasonable bounds.

        Args:
            long_price: The long/buy price.
            short_price: The short/sell price.

        Returns:
            CheckResult indicating whether spread is reasonable.
        """
        if long_price == short_price:
            return CheckResult.failure(
                message="Long and short prices are identical",
                details={"long_price": float(long_price), "short_price": float(short_price)},
            )

        # Calculate spread percentage
        avg_price = (long_price + short_price) / 2
        spread_percentage = abs(long_price - short_price) / avg_price

        # Note: We don't have min_spread_percentage in the new config, so we'll skip that check

        if spread_percentage > self._max_spread:
            return CheckResult.failure(
                message=(f"Spread {spread_percentage:.6f} above maximum {self._max_spread:.6f}"),
                details={
                    "spread_percentage": float(spread_percentage),
                    "max_spread_percentage": float(self._max_spread),
                },
            )

        return CheckResult.success("Spread reasonableness check passed")

    def _check_price_anomalies(
        self,
        symbol: str,
        long_price: Decimal,
        short_price: Decimal,
    ) -> CheckResult:
        """Check for price anomalies using historical data.

        Args:
            symbol: The trading symbol.
            long_price: The long/buy price.
            short_price: The short/sell price.

        Returns:
            CheckResult indicating whether any price anomalies were detected.
        """
        # Need at least 3 historical prices for statistical anomaly detection
        if (
            symbol not in self.price_history
            or len(self.price_history[symbol]) < MIN_HISTORICAL_PRICES_FOR_ANOMALY_DETECTION
        ):
            # Not enough historical data
            return CheckResult.success("Insufficient historical data for anomaly detection")

        history = self.price_history[symbol]
        avg_price = (long_price + short_price) / 2

        # Calculate historical average and standard deviation
        try:
            historical_prices = [float(p) for p in history]
            historical_mean = statistics.mean(historical_prices)
            historical_stdev = (
                statistics.stdev(historical_prices) if len(historical_prices) > 1 else 0
            )

            # Check if current price is an outlier (more than 3 standard deviations from mean)
            price_z_score = abs(float(avg_price) - historical_mean) / (historical_stdev + 1e-8)

            if price_z_score > self._z_score_threshold:
                return CheckResult.failure(
                    message=(
                        f"Price anomaly detected: z-score {price_z_score:.2f} > "
                        f"{self._z_score_threshold}"
                    ),
                    details={
                        "current_price": float(avg_price),
                        "historical_mean": historical_mean,
                        "historical_stdev": historical_stdev,
                        "z_score": price_z_score,
                    },
                )

            # Check for sudden price jumps
            if history:
                last_price = float(history[-1])
                price_change = abs(float(avg_price) - last_price) / last_price

                if price_change > float(self._max_deviation):
                    return CheckResult.failure(
                        message=(
                            f"Sudden price change detected: {price_change:.2%} > "
                            f"{self._max_deviation:.2%}"
                        ),
                        details={
                            "current_price": float(avg_price),
                            "last_price": last_price,
                            "price_change": price_change,
                            "max_deviation": float(self._max_deviation),
                        },
                    )

        except (ValueError, statistics.StatisticsError):
            # Handle edge cases in statistics calculation
            pass

        return CheckResult.success("No price anomalies detected")

    def _update_price_history(self, symbol: str, long_price: Decimal, short_price: Decimal) -> None:
        """Update price history for anomaly detection.

        Args:
            symbol: The trading symbol.
            long_price: The long/buy price.
            short_price: The short/sell price.
        """
        if symbol not in self.price_history:
            self.price_history[symbol] = []

        avg_price = (long_price + short_price) / 2
        self.price_history[symbol].append(avg_price)

        # Maintain history size limit
        max_history = int(self.max_history_size)
        if len(self.price_history[symbol]) > max_history:
            self.price_history[symbol] = self.price_history[symbol][-max_history:]

    def clear_price_history(self, symbol: str | None = None) -> None:
        """Clear price history for anomaly detection.

        Args:
            symbol: Symbol to clear history for, or None to clear all
        """
        if symbol:
            self.price_history.pop(symbol, None)
            self.logger.info("Cleared price history", symbol=symbol)
        else:
            self.price_history.clear()
            self.logger.info("Cleared all price history")

    def get_price_history(self, symbol: str) -> list[Decimal]:
        """Get price history for a symbol.

        Args:
            symbol: Symbol to get history for

        Returns:
            List of historical prices
        """
        return self.price_history.get(symbol, []).copy()

    def set_price_bounds(self, min_price: Decimal, max_price: Decimal) -> None:
        """Set price bounds for validation.

        Args:
            min_price: Minimum allowed price.
            max_price: Maximum allowed price.

        Raises:
            PriceSanityError: If min_price is not less than max_price.
        """
        if min_price >= max_price:
            msg = "min_price must be less than max_price"
            raise PriceSanityError(
                msg,
                metadata={"min_price": float(min_price), "max_price": float(max_price)},
                checker_name="PriceSanityChecker",
                check_type="price_sanity",
            )

        self._min_price = min_price
        self._max_price = max_price
        self.logger.info("Set price bounds", min_price=f"{min_price}", max_price=f"{max_price}")

    def set_spread_bounds(self, min_spread: Decimal, max_spread: Decimal) -> None:
        """Set spread bounds for validation.

        Args:
            min_spread: Minimum allowed spread percentage.
            max_spread: Maximum allowed spread percentage.

        Raises:
            PriceSanityError: If min_spread is not less than max_spread.
        """
        if min_spread >= max_spread:
            msg = "min_spread must be less than max_spread"
            raise PriceSanityError(
                msg,
                metadata={"min_spread": float(min_spread), "max_spread": float(max_spread)},
                checker_name="PriceSanityChecker",
                check_type="price_sanity",
            )

        # We only track max spread in the new config
        self._max_spread = max_spread
        self.logger.info(
            "Set spread bounds", min_spread=float(min_spread), max_spread=float(max_spread)
        )

    def _create_skip_result(self) -> CheckResult:
        """Create result for skipped check.

        Returns:
            CheckResult indicating the check was skipped.
        """
        return CheckResult.skip(
            message=f"{self.CHECKER_NAME} check skipped (disabled)",
        )

    def _create_error_result(self, error: Exception, execution_time: float) -> CheckResult:
        """Create result for failed check.

        Args:
            error: The exception that occurred.
            execution_time: Time taken for execution in seconds.

        Returns:
            CheckResult indicating the check encountered an error.
        """
        return CheckResult.error(
            message=f"{self.CHECKER_NAME} check error: {error}",
            details={"execution_time_ms": execution_time},
        )
