"""Funding Rate Validation System.

This module implements the validation system for funding rate predictions
against actual payments received/paid.
"""

import math
import operator
import time
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict, cast

from cyberdelta.config.models.config_models import AppSettings  # Correct path
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import Symbol, symbol
from cyberdelta.enums.exchange_names import ExchangeName


class HistorySeries(TypedDict):
    """Represent a time series of funding rate predictions or actuals.

    - timestamps: List of integer timestamps (ms since epoch)
    - datetimes: List of ISO-formatted datetime strings
    - rates: List of predicted or actual funding rates (float)
    """

    timestamps: list[int]
    datetimes: list[str]
    rates: list[float]


class PredictionHistory(TypedDict):
    """Structure for prediction history, containing both predictions and actuals.

    - predictions: HistorySeries of predicted rates
    - actuals: HistorySeries of actual rates
    """

    predictions: HistorySeries
    actuals: HistorySeries


class MergedDataEntry(TypedDict):
    """Represents a merged prediction-payment pair for metric calculation."""

    timestamp: int
    predicted_rate: float
    actual_rate: float
    method: str
    confidence: float
    error: float


class FundingRateValidator:
    """Validate funding rate data and predictions against actual payments.

    Tracks accuracy and provides metrics for improving predictions.
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize the FundingRateValidator.

        Args:
            config: Application configuration object

        """
        self.config = config
        self.logger = get_logger(__name__)

        # Simple in-memory storage for predictions and payments
        self.predictions: list[dict[str, Any]] = []
        self.payments: list[dict[str, Any]] = []

    def record_prediction(
        self,
        exchange: str,
        symbol: Symbol,
        predicted_rate: float,
        method: str = "api",
        confidence: float = 1.0,
    ) -> None:
        """Record a funding rate prediction.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            predicted_rate: Predicted funding rate
            method: Method used for prediction (api, model, etc.)
            confidence: Confidence level in the prediction (0-1)

        """
        timestamp = int(time.time() * 1000)

        prediction = {
            "timestamp": timestamp,
            "datetime": datetime.fromtimestamp(timestamp / 1000, UTC),
            "exchange": exchange,
            "symbol": symbol,
            "predicted_rate": predicted_rate,
            "method": method,
            "confidence": confidence,
        }

        self.predictions.append(prediction)
        self.logger.debug(
            "funding_rate_prediction_recorded",
            exchange=exchange,
            symbol=symbol,
            predicted_rate=predicted_rate,
            method=method,
            confidence=confidence,
            timestamp=timestamp,
            action="prediction_stored",
            message=(
                f"Recorded funding rate prediction: {exchange}/{symbol}, "
                f"rate={predicted_rate:.6f}, method={method}"
            ),
        )

    def record_payment(
        self,
        exchange: str,
        symbol: Symbol,
        actual_rate: float,
        payment_amount: float,
        position_size: float,
    ) -> None:
        """Record an actual funding payment.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            actual_rate: Actual funding rate that was applied
            payment_amount: Amount of funding paid/received
            position_size: Position size at time of payment

        """
        timestamp = int(time.time() * 1000)

        payment = {
            "timestamp": timestamp,
            "datetime": datetime.fromtimestamp(timestamp / 1000, UTC),
            "exchange": exchange,
            "symbol": symbol,
            "actual_rate": actual_rate,
            "payment_amount": payment_amount,
            "position_size": position_size,
        }

        self.payments.append(payment)
        self.logger.info(
            "funding_payment_recorded",
            exchange=exchange,
            symbol=symbol,
            actual_rate=actual_rate,
            payment_amount=payment_amount,
            position_size=position_size,
            timestamp=timestamp,
            action="payment_stored",
            message=(
                f"Recorded funding payment: {exchange}/{symbol}, "
                f"rate={actual_rate:.6f}, amount={payment_amount:.8f}"
            ),
        )

    def calculate_metrics(
        self,
        exchange: str,
        symbol: Symbol,
        days: int = 7,
    ) -> dict[str, float | None]:
        """Calculate prediction accuracy metrics.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            days: Number of days to include in calculation

        Returns:
            Dictionary with accuracy metrics (RMSE, MAE, bias, etc.)

        """
        # Calculate time threshold (milliseconds)
        threshold_time = datetime.now(UTC) - timedelta(days=days)
        threshold_ms = int(threshold_time.timestamp() * 1000)

        # Filter predictions for the given exchange, symbol, and time range
        filtered_predictions = [
            p
            for p in self.predictions
            if p["exchange"] == exchange
            and p["symbol"] == symbol
            and p["timestamp"] >= threshold_ms
        ]

        # Filter payments for the given exchange, symbol, and time range
        filtered_payments = [
            p
            for p in self.payments
            if p["exchange"] == exchange
            and p["symbol"] == symbol
            and p["timestamp"] >= threshold_ms
        ]

        # If we don't have enough data, return empty metrics
        if not filtered_predictions or not filtered_payments:
            self.logger.warning(
                "insufficient_data_for_metrics",
                exchange=exchange,
                symbol=symbol,
                prediction_count=len(filtered_predictions),
                payment_count=len(filtered_payments),
                days=days,
                action="returning_empty_metrics",
                message=f"Insufficient data to calculate metrics for {exchange}/{symbol}",
            )
            return {
                "rmse": None,
                "mae": None,
                "bias": None,
                "prediction_count": len(filtered_predictions),
                "payment_count": len(filtered_payments),
            }

        # Sort by timestamp
        filtered_predictions.sort(key=operator.itemgetter("timestamp"))
        filtered_payments.sort(key=operator.itemgetter("timestamp"))

        # Merge predictions with closest actual payments
        # For each payment, find the most recent prediction before the payment
        merged_data: list[MergedDataEntry] = []
        for payment in filtered_payments:
            payment_time = payment["timestamp"]
            actual_rate = payment["actual_rate"]

            # Find the most recent prediction before this payment
            relevant_predictions = [
                p for p in filtered_predictions if p["timestamp"] < payment_time
            ]
            if relevant_predictions:
                # Get the most recent prediction before the payment
                latest_prediction = max(relevant_predictions, key=operator.itemgetter("timestamp"))
                predicted_rate = latest_prediction["predicted_rate"]
                method = latest_prediction["method"]
                confidence = latest_prediction["confidence"]

                merged_data.append(
                    {
                        "timestamp": payment_time,
                        "predicted_rate": predicted_rate,
                        "actual_rate": actual_rate,
                        "method": method,
                        "confidence": confidence,
                        "error": predicted_rate - actual_rate,
                    },
                )

        # If we couldn't match any predictions with payments
        if not merged_data:
            self.logger.warning(
                "no_matching_prediction_payment_pairs",
                exchange=exchange,
                symbol=symbol,
                prediction_count=len(filtered_predictions),
                payment_count=len(filtered_payments),
                merged_count=0,
                action="returning_empty_metrics",
                message=f"No matching prediction-payment pairs for {exchange}/{symbol}",
            )
            return {
                "rmse": None,
                "mae": None,
                "bias": None,
                "prediction_count": len(filtered_predictions),
                "payment_count": len(filtered_payments),
            }

        # Calculate metrics
        errors: list[float] = [item["error"] for item in merged_data]
        rmse = math.sqrt(sum(e**2 for e in errors) / len(errors))
        mae = sum(abs(e) for e in errors) / len(errors)
        bias = sum(errors) / len(errors)

        metrics = {
            "rmse": rmse,
            "mae": mae,
            "bias": bias,
            "prediction_count": float(len(filtered_predictions)),
            "payment_count": float(len(filtered_payments)),
            "matched_count": float(len(merged_data)),
        }

        self.logger.info(
            "funding_rate_metrics_calculated",
            exchange=exchange,
            symbol=symbol,
            rmse=rmse,
            mae=mae,
            bias=bias,
            prediction_count=len(filtered_predictions),
            payment_count=len(filtered_payments),
            matched_count=len(merged_data),
            days=days,
            action="metrics_computed",
            message=(
                f"Calculated metrics for {exchange}/{symbol}: "
                f"RMSE={rmse:.6f}, MAE={mae:.6f}, Bias={bias:.6f}"
            ),
        )
        return cast("dict[str, float | None]", metrics)

    def get_validation_report(self, days: int = 7) -> dict[str, dict[str, dict[str, float | None]]]:
        """Generate a comprehensive validation report.

        Args:
            days: Number of days to include in the report

        Returns:
            Dictionary with validation metrics by exchange and symbol

        """
        report: dict[str, dict[str, dict[str, float | None]]] = {}

        # Get unique exchange-symbol pairs from all predictions and payments
        exchange_symbols: set[tuple[ExchangeName, Symbol]] = set()

        exchange_symbols.update(
            (prediction["exchange"], prediction["symbol"]) for prediction in self.predictions
        )

        exchange_symbols.update(
            (payment["exchange"], payment["symbol"]) for payment in self.payments
        )

        # Generate report for each pair
        for exchange, symbol_obj in exchange_symbols:
            if exchange not in report:
                report[exchange] = {}

            metrics = self.calculate_metrics(exchange, symbol_obj, days)
            report[exchange][symbol_obj.value] = metrics

        return report

    def get_recent_predictions(
        self,
        exchange: str | None = None,
        symbol: Symbol | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get recent funding rate predictions.

        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            limit: Maximum number of records to return

        Returns:
            List of prediction records

        """
        # Filter predictions
        filtered_predictions = self.predictions

        if exchange:
            filtered_predictions = [p for p in filtered_predictions if p["exchange"] == exchange]

        if symbol:
            filtered_predictions = [p for p in filtered_predictions if p["symbol"] == symbol]

        # Sort by timestamp (newest first) and apply limit
        sorted_predictions = sorted(
            filtered_predictions,
            key=operator.itemgetter("timestamp"),
            reverse=True,
        )
        return sorted_predictions[:limit]

    def get_recent_payments(
        self,
        exchange: str | None = None,
        symbol: Symbol | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get recent funding payments.

        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            limit: Maximum number of records to return

        Returns:
            List of payment records

        """
        # Filter payments
        filtered_payments = self.payments

        if exchange:
            filtered_payments = [p for p in filtered_payments if p["exchange"] == exchange]

        if symbol:
            filtered_payments = [p for p in filtered_payments if p["symbol"] == symbol]

        # Sort by timestamp (newest first) and apply limit
        sorted_payments = sorted(
            filtered_payments,
            key=operator.itemgetter("timestamp"),
            reverse=True,
        )
        return sorted_payments[:limit]

    def get_prediction_history(
        self,
        exchange: str,
        symbol: Symbol,
        days: int = 30,
    ) -> PredictionHistory:
        """Get prediction history for a specific exchange and symbol.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            days: Number of days of history to retrieve

        Returns:
            Dictionary with timestamp, predicted, and actual rate lists

        """
        # Calculate time threshold
        threshold_time = datetime.now(UTC) - timedelta(days=days)
        threshold_ms = int(threshold_time.timestamp() * 1000)

        # Filter predictions
        filtered_predictions = [
            p
            for p in self.predictions
            if p["exchange"] == exchange
            and p["symbol"] == symbol
            and p["timestamp"] >= threshold_ms
        ]

        # Filter payments
        filtered_payments = [
            p
            for p in self.payments
            if p["exchange"] == exchange
            and p["symbol"] == symbol
            and p["timestamp"] >= threshold_ms
        ]

        # Sort by timestamp
        filtered_predictions.sort(key=operator.itemgetter("timestamp"))
        filtered_payments.sort(key=operator.itemgetter("timestamp"))

        return {
            "predictions": {
                "timestamps": [p["timestamp"] for p in filtered_predictions],
                "datetimes": [p["datetime"] for p in filtered_predictions],
                "rates": [p["predicted_rate"] for p in filtered_predictions],
            },
            "actuals": {
                "timestamps": [p["timestamp"] for p in filtered_payments],
                "datetimes": [p["datetime"] for p in filtered_payments],
                "rates": [p["actual_rate"] for p in filtered_payments],
            },
        }

    def clear_old_data(self, days_to_keep: int = 90) -> None:
        """Remove data older than the specified number of days.

        Args:
            days_to_keep: Number of days of data to retain

        """
        threshold_time = datetime.now(UTC) - timedelta(days=days_to_keep)
        threshold_ms = int(threshold_time.timestamp() * 1000)

        # Filter out old predictions
        self.predictions = [p for p in self.predictions if p["timestamp"] >= threshold_ms]

        # Filter out old payments
        self.payments = [p for p in self.payments if p["timestamp"] >= threshold_ms]

        self.logger.info(
            "old_data_cleared",
            days_to_keep=days_to_keep,
            threshold_time=threshold_time.isoformat(),
            remaining_predictions=len(self.predictions),
            remaining_payments=len(self.payments),
            action="data_cleanup_completed",
            message=(
                f"Cleared data older than {days_to_keep} days. "
                f"Remaining: {len(self.predictions)} predictions, {len(self.payments)} payments"
            ),
        )

    def get_symbol_metrics(self, exchange: str, symbol: Symbol) -> dict[str, float | None]:
        """Protocol-compatible method for RiskManager; delegates to calculate_metrics.

        Returns:
            Dictionary containing calculated metrics for the symbol
        """
        return self.calculate_metrics(exchange, symbol)
