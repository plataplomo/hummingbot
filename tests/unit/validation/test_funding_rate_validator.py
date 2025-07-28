"""Unit tests for the funding rate validator module.

Tests funding rate validation functionality for predictions and payments.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.validation.funding_rate_validator import FundingRateValidator


@pytest.fixture
def mock_config() -> AppSettings:
    """Create a mock configuration for testing.

    Returns:
        AppSettings: Mock configuration instance for testing.
    """
    return Mock(spec=AppSettings)


@pytest.fixture
def validator(mock_config: AppSettings) -> FundingRateValidator:
    """Create a funding rate validator instance for testing.

    Returns:
        FundingRateValidator: Configured validator instance for testing.
    """
    return FundingRateValidator(mock_config)


@pytest.fixture
def sample_predictions() -> list[dict[str, Any]]:
    """Create sample prediction data for testing.

    Returns:
        list[dict[str, Any]]: List of sample prediction data dictionaries.
    """
    base_time = int(time.time() * 1000)
    return [
        {
            "timestamp": base_time - 3600000,  # 1 hour ago
            "datetime": datetime.fromtimestamp((base_time - 3600000) / 1000, UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        },
        {
            "timestamp": base_time - 1800000,  # 30 minutes ago
            "datetime": datetime.fromtimestamp((base_time - 1800000) / 1000, UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "predicted_rate": 0.00015,
            "method": "model",
            "confidence": 0.8,
        },
    ]


@pytest.fixture
def sample_payments() -> list[dict[str, Any]]:
    """Create sample payment data for testing.

    Returns:
        list[dict[str, Any]]: List of sample payment data dictionaries.
    """
    base_time = int(time.time() * 1000)
    return [
        {
            "timestamp": base_time - 600000,  # 10 minutes ago
            "datetime": datetime.fromtimestamp((base_time - 600000) / 1000, UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "actual_rate": 0.00012,
            "payment_amount": 1.2,
            "position_size": 10000.0,
        },
    ]


class TestFundingRateValidatorInit:
    """Test suite for FundingRateValidator initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success(self, mock_config: AppSettings) -> None:
        """Test successful initialization of FundingRateValidator."""
        # Act
        validator = FundingRateValidator(mock_config)

        # Assert
        assert validator.config == mock_config
        assert hasattr(validator, "logger")
        assert isinstance(validator.predictions, list)
        assert isinstance(validator.payments, list)
        assert len(validator.predictions) == 0
        assert len(validator.payments) == 0


class TestRecordPrediction:
    """Test suite for record_prediction method."""

    # ==================== SUCCESS CASES ====================

    def test_record_prediction_success_basic(
        self, validator: FundingRateValidator, mock_time_patch: MagicMock
    ) -> None:
        """Test successful recording of a basic prediction."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"
        predicted_rate = 0.0001
        mock_time_patch.return_value = 1640995200  # Fixed timestamp

        # Act
        validator.record_prediction(exchange, symbol, predicted_rate)

        # Assert
        assert len(validator.predictions) == 1
        prediction = validator.predictions[0]
        assert prediction["exchange"] == exchange
        assert prediction["symbol"] == symbol
        assert prediction["predicted_rate"] == predicted_rate
        assert prediction["method"] == "api"  # Default
        assert prediction["confidence"] == 1.0  # Default
        assert prediction["timestamp"] == 1640995200000

    def test_record_prediction_success_with_optional_params(
        self, validator: FundingRateValidator
    ) -> None:
        """Test successful recording of prediction with optional parameters."""
        # Arrange
        exchange = "backpack"
        symbol = "ETH-PERP"
        predicted_rate = 0.00025
        method = "model"
        confidence = 0.85

        # Act
        validator.record_prediction(exchange, symbol, predicted_rate, method, confidence)

        # Assert
        assert len(validator.predictions) == 1
        prediction = validator.predictions[0]
        assert prediction["exchange"] == exchange
        assert prediction["symbol"] == symbol
        assert prediction["predicted_rate"] == predicted_rate
        assert prediction["method"] == method
        assert prediction["confidence"] == confidence
        assert isinstance(prediction["datetime"], datetime)

    def test_record_prediction_success_multiple(self, validator: FundingRateValidator) -> None:
        """Test recording multiple predictions."""
        # Act
        validator.record_prediction("exchange1", "BTC-PERP", 0.0001)
        validator.record_prediction("exchange2", "ETH-PERP", 0.0002)

        # Assert
        assert len(validator.predictions) == 2
        assert validator.predictions[0]["exchange"] == "exchange1"
        assert validator.predictions[1]["exchange"] == "exchange2"

    # ==================== EDGE CASES ====================

    def test_record_prediction_edge_zero_rate(self, validator: FundingRateValidator) -> None:
        """Test recording prediction with zero rate."""
        # Act
        validator.record_prediction("hyperliquid", "BTC-PERP", 0.0)

        # Assert
        assert len(validator.predictions) == 1
        assert validator.predictions[0]["predicted_rate"] == 0.0

    def test_record_prediction_edge_negative_rate(self, validator: FundingRateValidator) -> None:
        """Test recording prediction with negative rate."""
        # Act
        validator.record_prediction("hyperliquid", "BTC-PERP", -0.0001)

        # Assert
        assert len(validator.predictions) == 1
        assert validator.predictions[0]["predicted_rate"] == -0.0001

    def test_record_prediction_edge_extreme_confidence(
        self, validator: FundingRateValidator
    ) -> None:
        """Test recording prediction with extreme confidence values."""
        # Act
        validator.record_prediction("hyperliquid", "BTC-PERP", 0.0001, confidence=0.0)
        validator.record_prediction("hyperliquid", "ETH-PERP", 0.0002, confidence=1.0)

        # Assert
        assert len(validator.predictions) == 2
        assert validator.predictions[0]["confidence"] == 0.0
        assert validator.predictions[1]["confidence"] == 1.0


class TestRecordPayment:
    """Test suite for record_payment method."""

    # ==================== SUCCESS CASES ====================

    def test_record_payment_success_basic(
        self, validator: FundingRateValidator, mock_time_patch: MagicMock
    ) -> None:
        """Test successful recording of a basic payment."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"
        actual_rate = 0.00012
        payment_amount = 1.2
        position_size = 10000.0
        mock_time_patch.return_value = 1640995200  # Fixed timestamp

        # Act
        validator.record_payment(exchange, symbol, actual_rate, payment_amount, position_size)

        # Assert
        assert len(validator.payments) == 1
        payment = validator.payments[0]
        assert payment["exchange"] == exchange
        assert payment["symbol"] == symbol
        assert payment["actual_rate"] == actual_rate
        assert payment["payment_amount"] == payment_amount
        assert payment["position_size"] == position_size
        assert payment["timestamp"] == 1640995200000
        assert isinstance(payment["datetime"], datetime)

    def test_record_payment_success_multiple(self, validator: FundingRateValidator) -> None:
        """Test recording multiple payments."""
        # Act
        validator.record_payment("exchange1", "BTC-PERP", 0.0001, 1.0, 10000.0)
        validator.record_payment("exchange2", "ETH-PERP", 0.0002, 2.0, 5000.0)

        # Assert
        assert len(validator.payments) == 2
        assert validator.payments[0]["exchange"] == "exchange1"
        assert validator.payments[1]["exchange"] == "exchange2"

    # ==================== EDGE CASES ====================

    def test_record_payment_edge_zero_values(self, validator: FundingRateValidator) -> None:
        """Test recording payment with zero values."""
        # Act
        validator.record_payment("hyperliquid", "BTC-PERP", 0.0, 0.0, 0.0)

        # Assert
        assert len(validator.payments) == 1
        payment = validator.payments[0]
        assert payment["actual_rate"] == 0.0
        assert payment["payment_amount"] == 0.0
        assert payment["position_size"] == 0.0

    def test_record_payment_edge_negative_values(self, validator: FundingRateValidator) -> None:
        """Test recording payment with negative values."""
        # Act
        validator.record_payment("hyperliquid", "BTC-PERP", -0.0001, -1.5, 10000.0)

        # Assert
        assert len(validator.payments) == 1
        payment = validator.payments[0]
        assert payment["actual_rate"] == -0.0001
        assert payment["payment_amount"] == -1.5
        assert payment["position_size"] == 10000.0


class TestCalculateMetrics:
    """Test suite for calculate_metrics method."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_metrics_success_with_data(self, validator: FundingRateValidator) -> None:
        """Test successful calculation of metrics with sufficient data."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"

        # Add prediction data (older than payment)
        base_time = int(time.time() * 1000)
        validator.predictions.append({
            "timestamp": base_time - 3600000,  # 1 hour ago
            "datetime": datetime.fromtimestamp((base_time - 3600000) / 1000, UTC),
            "exchange": exchange,
            "symbol": symbol,
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Add payment data (newer than prediction)
        validator.payments.append({
            "timestamp": base_time - 1800000,  # 30 minutes ago
            "datetime": datetime.fromtimestamp((base_time - 1800000) / 1000, UTC),
            "exchange": exchange,
            "symbol": symbol,
            "actual_rate": 0.00012,
            "payment_amount": 1.2,
            "position_size": 10000.0,
        })

        # Act
        metrics = validator.calculate_metrics(exchange, symbol)

        # Assert
        assert isinstance(metrics, dict)
        assert metrics["rmse"] is not None
        assert metrics["mae"] is not None
        assert metrics["bias"] is not None
        assert metrics["prediction_count"] == 1.0
        assert metrics["payment_count"] == 1.0
        assert metrics["matched_count"] == 1.0

        # Check specific metric values
        expected_error = 0.0001 - 0.00012  # -0.00002
        assert abs(metrics["rmse"] - abs(expected_error)) < 1e-10
        assert abs(metrics["mae"] - abs(expected_error)) < 1e-10
        assert abs(metrics["bias"] - expected_error) < 1e-10

    def test_calculate_metrics_success_multiple_data_points(
        self, validator: FundingRateValidator
    ) -> None:
        """Test calculating metrics with multiple prediction-payment pairs."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"
        base_time = int(time.time() * 1000)

        # Add multiple predictions
        predictions = [
            (base_time - 7200000, 0.0001),  # 2 hours ago
            (base_time - 5400000, 0.00015),  # 1.5 hours ago
            (base_time - 3600000, 0.00012),  # 1 hour ago
        ]

        for timestamp, rate in predictions:
            validator.predictions.append({
                "timestamp": timestamp,
                "datetime": datetime.fromtimestamp(timestamp / 1000, UTC),
                "exchange": exchange,
                "symbol": symbol,
                "predicted_rate": rate,
                "method": "api",
                "confidence": 0.9,
            })

        # Add multiple payments
        payments = [
            (base_time - 4800000, 0.00011),  # 80 minutes ago
            (base_time - 1800000, 0.00013),  # 30 minutes ago
        ]

        for timestamp, rate in payments:
            validator.payments.append({
                "timestamp": timestamp,
                "datetime": datetime.fromtimestamp(timestamp / 1000, UTC),
                "exchange": exchange,
                "symbol": symbol,
                "actual_rate": rate,
                "payment_amount": 1.0,
                "position_size": 10000.0,
            })

        # Act
        metrics = validator.calculate_metrics(exchange, symbol)

        # Assert
        assert metrics["rmse"] is not None
        assert metrics["mae"] is not None
        assert metrics["bias"] is not None
        assert metrics["matched_count"] == 2.0  # Two matched pairs

    # ==================== EDGE CASES ====================

    def test_calculate_metrics_edge_no_predictions(self, validator: FundingRateValidator) -> None:
        """Test calculating metrics when no predictions exist."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"

        # Add only payment data
        validator.payments.append({
            "timestamp": int(time.time() * 1000),
            "datetime": datetime.now(UTC),
            "exchange": exchange,
            "symbol": symbol,
            "actual_rate": 0.00012,
            "payment_amount": 1.2,
            "position_size": 10000.0,
        })

        # Act
        metrics = validator.calculate_metrics(exchange, symbol)

        # Assert
        assert metrics["rmse"] is None
        assert metrics["mae"] is None
        assert metrics["bias"] is None
        assert metrics["prediction_count"] == 0
        assert metrics["payment_count"] == 1

    def test_calculate_metrics_edge_no_payments(self, validator: FundingRateValidator) -> None:
        """Test calculating metrics when no payments exist."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"

        # Add only prediction data
        validator.predictions.append({
            "timestamp": int(time.time() * 1000),
            "datetime": datetime.now(UTC),
            "exchange": exchange,
            "symbol": symbol,
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Act
        metrics = validator.calculate_metrics(exchange, symbol)

        # Assert
        assert metrics["rmse"] is None
        assert metrics["mae"] is None
        assert metrics["bias"] is None
        assert metrics["prediction_count"] == 1
        assert metrics["payment_count"] == 0

    def test_calculate_metrics_edge_no_matching_pairs(
        self, validator: FundingRateValidator
    ) -> None:
        """Test calculating metrics when predictions and payments don't match temporally."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"
        base_time = int(time.time() * 1000)

        # Add prediction AFTER payment (no valid pairs)
        validator.predictions.append({
            "timestamp": base_time - 1800000,  # 30 minutes ago
            "datetime": datetime.fromtimestamp((base_time - 1800000) / 1000, UTC),
            "exchange": exchange,
            "symbol": symbol,
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Add payment BEFORE prediction
        validator.payments.append({
            "timestamp": base_time - 3600000,  # 1 hour ago
            "datetime": datetime.fromtimestamp((base_time - 3600000) / 1000, UTC),
            "exchange": exchange,
            "symbol": symbol,
            "actual_rate": 0.00012,
            "payment_amount": 1.2,
            "position_size": 10000.0,
        })

        # Act
        metrics = validator.calculate_metrics(exchange, symbol)

        # Assert
        assert metrics["rmse"] is None
        assert metrics["mae"] is None
        assert metrics["bias"] is None
        assert metrics["prediction_count"] == 1
        assert metrics["payment_count"] == 1

    def test_calculate_metrics_edge_time_filter(self, validator: FundingRateValidator) -> None:
        """Test calculating metrics with time filtering."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"
        base_time = int(time.time() * 1000)

        # Add old prediction (should be filtered out)
        validator.predictions.append({
            "timestamp": base_time - (10 * 24 * 3600 * 1000),  # 10 days ago
            "datetime": datetime.fromtimestamp((base_time - (10 * 24 * 3600 * 1000)) / 1000, UTC),
            "exchange": exchange,
            "symbol": symbol,
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Act
        metrics = validator.calculate_metrics(exchange, symbol, days=7)  # Only last 7 days

        # Assert
        assert metrics["rmse"] is None
        assert metrics["prediction_count"] == 0


class TestGetValidationReport:
    """Test suite for get_validation_report method."""

    # ==================== SUCCESS CASES ====================

    def test_get_validation_report_success_with_data(self, validator: FundingRateValidator) -> None:
        """Test generating validation report with data."""
        # Arrange
        base_time = int(time.time() * 1000)

        # Add data for multiple exchange-symbol pairs
        exchanges_symbols = [("hyperliquid", "BTC-PERP"), ("backpack", "ETH-PERP")]

        for exchange, symbol in exchanges_symbols:
            validator.predictions.append({
                "timestamp": base_time - 3600000,
                "datetime": datetime.fromtimestamp((base_time - 3600000) / 1000, UTC),
                "exchange": exchange,
                "symbol": symbol,
                "predicted_rate": 0.0001,
                "method": "api",
                "confidence": 0.9,
            })

            validator.payments.append({
                "timestamp": base_time - 1800000,
                "datetime": datetime.fromtimestamp((base_time - 1800000) / 1000, UTC),
                "exchange": exchange,
                "symbol": symbol,
                "actual_rate": 0.00012,
                "payment_amount": 1.0,
                "position_size": 10000.0,
            })

        # Act
        report = validator.get_validation_report()

        # Assert
        assert isinstance(report, dict)
        assert "hyperliquid" in report
        assert "backpack" in report
        assert "BTC-PERP" in report["hyperliquid"]
        assert "ETH-PERP" in report["backpack"]

        # Check metrics structure
        btc_metrics = report["hyperliquid"]["BTC-PERP"]
        assert "rmse" in btc_metrics
        assert "mae" in btc_metrics
        assert "bias" in btc_metrics

    # ==================== EDGE CASES ====================

    def test_get_validation_report_edge_no_data(self, validator: FundingRateValidator) -> None:
        """Test generating validation report with no data."""
        # Act
        report = validator.get_validation_report()

        # Assert
        assert isinstance(report, dict)
        assert len(report) == 0

    def test_get_validation_report_edge_custom_days(self, validator: FundingRateValidator) -> None:
        """Test generating validation report with custom day range."""
        # Arrange
        base_time = int(time.time() * 1000)
        validator.predictions.append({
            "timestamp": base_time - 3600000,
            "datetime": datetime.fromtimestamp((base_time - 3600000) / 1000, UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Act
        report = validator.get_validation_report(days=1)

        # Assert
        assert isinstance(report, dict)
        assert "hyperliquid" in report


class TestGetRecentPredictions:
    """Test suite for get_recent_predictions method."""

    # ==================== SUCCESS CASES ====================

    def test_get_recent_predictions_success_all(self, validator: FundingRateValidator) -> None:
        """Test getting all recent predictions."""
        # Arrange
        base_time = int(time.time() * 1000)
        predictions = [
            ("hyperliquid", "BTC-PERP", base_time - 3600000),
            ("hyperliquid", "ETH-PERP", base_time - 1800000),
            ("backpack", "BTC-PERP", base_time - 900000),
        ]

        for exchange, symbol, timestamp in predictions:
            validator.predictions.append({
                "timestamp": timestamp,
                "datetime": datetime.fromtimestamp(timestamp / 1000, UTC),
                "exchange": exchange,
                "symbol": symbol,
                "predicted_rate": 0.0001,
                "method": "api",
                "confidence": 0.9,
            })

        # Act
        recent = validator.get_recent_predictions()

        # Assert
        assert len(recent) == 3
        # Should be sorted by timestamp descending (newest first)
        assert recent[0]["timestamp"] > recent[1]["timestamp"]
        assert recent[1]["timestamp"] > recent[2]["timestamp"]

    def test_get_recent_predictions_success_with_exchange_filter(
        self, validator: FundingRateValidator
    ) -> None:
        """Test getting recent predictions filtered by exchange."""
        # Arrange
        base_time = int(time.time() * 1000)
        exchanges = ["hyperliquid", "backpack", "hyperliquid"]

        for i, exchange in enumerate(exchanges):
            validator.predictions.append({
                "timestamp": base_time - (i * 1800000),
                "datetime": datetime.fromtimestamp((base_time - (i * 1800000)) / 1000, UTC),
                "exchange": exchange,
                "symbol": "BTC-PERP",
                "predicted_rate": 0.0001,
                "method": "api",
                "confidence": 0.9,
            })

        # Act
        recent = validator.get_recent_predictions(exchange="hyperliquid")

        # Assert
        assert len(recent) == 2
        for prediction in recent:
            assert prediction["exchange"] == "hyperliquid"

    def test_get_recent_predictions_success_with_symbol_filter(
        self, validator: FundingRateValidator
    ) -> None:
        """Test getting recent predictions filtered by symbol."""
        # Arrange
        base_time = int(time.time() * 1000)
        symbols = ["BTC-PERP", "ETH-PERP", "BTC-PERP"]

        for i, symbol in enumerate(symbols):
            validator.predictions.append({
                "timestamp": base_time - (i * 1800000),
                "datetime": datetime.fromtimestamp((base_time - (i * 1800000)) / 1000, UTC),
                "exchange": "hyperliquid",
                "symbol": symbol,
                "predicted_rate": 0.0001,
                "method": "api",
                "confidence": 0.9,
            })

        # Act
        recent = validator.get_recent_predictions(symbol="BTC-PERP")

        # Assert
        assert len(recent) == 2
        for prediction in recent:
            assert prediction["symbol"] == "BTC-PERP"

    def test_get_recent_predictions_success_with_limit(
        self, validator: FundingRateValidator
    ) -> None:
        """Test getting recent predictions with limit."""
        # Arrange
        base_time = int(time.time() * 1000)
        for i in range(5):
            validator.predictions.append({
                "timestamp": base_time - (i * 1800000),
                "datetime": datetime.fromtimestamp((base_time - (i * 1800000)) / 1000, UTC),
                "exchange": "hyperliquid",
                "symbol": "BTC-PERP",
                "predicted_rate": 0.0001,
                "method": "api",
                "confidence": 0.9,
            })

        # Act
        recent = validator.get_recent_predictions(limit=3)

        # Assert
        assert len(recent) == 3

    # ==================== EDGE CASES ====================

    def test_get_recent_predictions_edge_no_data(self, validator: FundingRateValidator) -> None:
        """Test getting recent predictions when no data exists."""
        # Act
        recent = validator.get_recent_predictions()

        # Assert
        assert recent == []

    def test_get_recent_predictions_edge_no_matches(self, validator: FundingRateValidator) -> None:
        """Test getting recent predictions with filters that match nothing."""
        # Arrange
        validator.predictions.append({
            "timestamp": int(time.time() * 1000),
            "datetime": datetime.now(UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Act
        recent = validator.get_recent_predictions(exchange="nonexistent")

        # Assert
        assert recent == []


class TestGetRecentPayments:
    """Test suite for get_recent_payments method."""

    # ==================== SUCCESS CASES ====================

    def test_get_recent_payments_success_all(self, validator: FundingRateValidator) -> None:
        """Test getting all recent payments."""
        # Arrange
        base_time = int(time.time() * 1000)
        for i in range(3):
            validator.payments.append({
                "timestamp": base_time - (i * 1800000),
                "datetime": datetime.fromtimestamp((base_time - (i * 1800000)) / 1000, UTC),
                "exchange": "hyperliquid",
                "symbol": "BTC-PERP",
                "actual_rate": 0.0001,
                "payment_amount": 1.0,
                "position_size": 10000.0,
            })

        # Act
        recent = validator.get_recent_payments()

        # Assert
        assert len(recent) == 3
        # Should be sorted by timestamp descending (newest first)
        assert recent[0]["timestamp"] > recent[1]["timestamp"]

    def test_get_recent_payments_success_with_filters(
        self, validator: FundingRateValidator
    ) -> None:
        """Test getting recent payments with exchange and symbol filters."""
        # Arrange
        base_time = int(time.time() * 1000)
        test_data = [
            ("hyperliquid", "BTC-PERP"),
            ("hyperliquid", "ETH-PERP"),
            ("backpack", "BTC-PERP"),
        ]

        for i, (exchange, symbol) in enumerate(test_data):
            validator.payments.append({
                "timestamp": base_time - (i * 1800000),
                "datetime": datetime.fromtimestamp((base_time - (i * 1800000)) / 1000, UTC),
                "exchange": exchange,
                "symbol": symbol,
                "actual_rate": 0.0001,
                "payment_amount": 1.0,
                "position_size": 10000.0,
            })

        # Act
        recent = validator.get_recent_payments(exchange="hyperliquid", symbol="BTC-PERP")

        # Assert
        assert len(recent) == 1
        assert recent[0]["exchange"] == "hyperliquid"
        assert recent[0]["symbol"] == "BTC-PERP"

    # ==================== EDGE CASES ====================

    def test_get_recent_payments_edge_no_data(self, validator: FundingRateValidator) -> None:
        """Test getting recent payments when no data exists."""
        # Act
        recent = validator.get_recent_payments()

        # Assert
        assert recent == []


class TestGetPredictionHistory:
    """Test suite for get_prediction_history method."""

    # ==================== SUCCESS CASES ====================

    def test_get_prediction_history_success(self, validator: FundingRateValidator) -> None:
        """Test getting prediction history with data."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"
        base_time = int(time.time() * 1000)

        # Add predictions
        validator.predictions.append({
            "timestamp": base_time - 3600000,
            "datetime": datetime.fromtimestamp((base_time - 3600000) / 1000, UTC).isoformat(),
            "exchange": exchange,
            "symbol": symbol,
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Add payments
        validator.payments.append({
            "timestamp": base_time - 1800000,
            "datetime": datetime.fromtimestamp((base_time - 1800000) / 1000, UTC).isoformat(),
            "exchange": exchange,
            "symbol": symbol,
            "actual_rate": 0.00012,
            "payment_amount": 1.0,
            "position_size": 10000.0,
        })

        # Act
        history = validator.get_prediction_history(exchange, symbol)

        # Assert
        assert "predictions" in history
        assert "actuals" in history

        predictions = history["predictions"]
        assert len(predictions["timestamps"]) == 1
        assert len(predictions["datetimes"]) == 1
        assert len(predictions["rates"]) == 1
        assert predictions["rates"][0] == 0.0001

        actuals = history["actuals"]
        assert len(actuals["timestamps"]) == 1
        assert len(actuals["datetimes"]) == 1
        assert len(actuals["rates"]) == 1
        assert actuals["rates"][0] == 0.00012

    # ==================== EDGE CASES ====================

    def test_get_prediction_history_edge_no_data(self, validator: FundingRateValidator) -> None:
        """Test getting prediction history with no data."""
        # Act
        history = validator.get_prediction_history("hyperliquid", "BTC-PERP")

        # Assert
        assert "predictions" in history
        assert "actuals" in history
        assert len(history["predictions"]["timestamps"]) == 0
        assert len(history["actuals"]["timestamps"]) == 0

    def test_get_prediction_history_edge_custom_days(self, validator: FundingRateValidator) -> None:
        """Test getting prediction history with custom day range."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"
        base_time = int(time.time() * 1000)

        # Add old prediction (should be filtered out)
        validator.predictions.append({
            "timestamp": base_time - (10 * 24 * 3600 * 1000),  # 10 days ago
            "datetime": datetime.fromtimestamp(
                (base_time - (10 * 24 * 3600 * 1000)) / 1000, UTC
            ).isoformat(),
            "exchange": exchange,
            "symbol": symbol,
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Act
        history = validator.get_prediction_history(exchange, symbol, days=7)

        # Assert
        assert len(history["predictions"]["timestamps"]) == 0  # Filtered out


class TestClearOldData:
    """Test suite for clear_old_data method."""

    # ==================== SUCCESS CASES ====================

    def test_clear_old_data_success(self, validator: FundingRateValidator) -> None:
        """Test clearing old data successfully."""
        # Arrange
        base_time = int(time.time() * 1000)

        # Add old data (should be removed)
        validator.predictions.append({
            "timestamp": base_time - (100 * 24 * 3600 * 1000),  # 100 days ago
            "datetime": datetime.fromtimestamp((base_time - (100 * 24 * 3600 * 1000)) / 1000, UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Add recent data (should be kept)
        validator.predictions.append({
            "timestamp": base_time - 3600000,  # 1 hour ago
            "datetime": datetime.fromtimestamp((base_time - 3600000) / 1000, UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "predicted_rate": 0.0002,
            "method": "api",
            "confidence": 0.9,
        })

        # Add old payment
        validator.payments.append({
            "timestamp": base_time - (100 * 24 * 3600 * 1000),  # 100 days ago
            "datetime": datetime.fromtimestamp((base_time - (100 * 24 * 3600 * 1000)) / 1000, UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "actual_rate": 0.0001,
            "payment_amount": 1.0,
            "position_size": 10000.0,
        })

        # Act
        validator.clear_old_data(days_to_keep=90)

        # Assert
        assert len(validator.predictions) == 1  # Only recent prediction kept
        assert validator.predictions[0]["predicted_rate"] == 0.0002
        assert len(validator.payments) == 0  # Old payment removed

    # ==================== EDGE CASES ====================

    def test_clear_old_data_edge_no_data_to_clear(self, validator: FundingRateValidator) -> None:
        """Test clearing old data when no old data exists."""
        # Arrange
        base_time = int(time.time() * 1000)
        validator.predictions.append({
            "timestamp": base_time - 3600000,  # 1 hour ago
            "datetime": datetime.fromtimestamp((base_time - 3600000) / 1000, UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Act
        validator.clear_old_data(days_to_keep=90)

        # Assert
        assert len(validator.predictions) == 1  # Data preserved

    def test_clear_old_data_edge_all_data_old(self, validator: FundingRateValidator) -> None:
        """Test clearing old data when all data is old."""
        # Arrange
        base_time = int(time.time() * 1000)
        validator.predictions.append({
            "timestamp": base_time - (100 * 24 * 3600 * 1000),  # 100 days ago
            "datetime": datetime.fromtimestamp((base_time - (100 * 24 * 3600 * 1000)) / 1000, UTC),
            "exchange": "hyperliquid",
            "symbol": "BTC-PERP",
            "predicted_rate": 0.0001,
            "method": "api",
            "confidence": 0.9,
        })

        # Act
        validator.clear_old_data(days_to_keep=30)

        # Assert
        assert len(validator.predictions) == 0  # All data cleared


class TestGetSymbolMetrics:
    """Test suite for get_symbol_metrics method."""

    # ==================== SUCCESS CASES ====================

    def test_get_symbol_metrics_success_delegates_to_calculate_metrics(
        self, validator: FundingRateValidator
    ) -> None:
        """Test that get_symbol_metrics delegates to calculate_metrics."""
        # Arrange
        exchange = "hyperliquid"
        symbol = "BTC-PERP"

        # Mock calculate_metrics to verify delegation
        with patch.object(validator, "calculate_metrics") as mock_calculate:
            mock_calculate.return_value = {"rmse": 0.0001, "mae": 0.0001, "bias": 0.0}

            # Act
            result = validator.get_symbol_metrics(exchange, symbol)

            # Assert
            mock_calculate.assert_called_once_with(exchange, symbol)
            assert result == {"rmse": 0.0001, "mae": 0.0001, "bias": 0.0}


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("predicted_rate", "actual_rate", "expected_error"),
    [
        (0.0001, 0.00012, -0.00002),  # Under-prediction
        (0.0002, 0.00015, 0.00005),  # Over-prediction
        (0.0001, 0.0001, 0.0),  # Perfect prediction
        (-0.0001, -0.00005, -0.00005),  # Negative rates
    ],
)
def test_metric_calculation_parametrized(
    validator: FundingRateValidator,
    predicted_rate: float,
    actual_rate: float,
    expected_error: float,
) -> None:
    """Test metric calculations for various prediction-actual rate combinations."""
    # Arrange
    exchange = "hyperliquid"
    symbol = "BTC-PERP"
    base_time = int(time.time() * 1000)

    # Add prediction
    validator.predictions.append({
        "timestamp": base_time - 3600000,
        "datetime": datetime.fromtimestamp((base_time - 3600000) / 1000, UTC),
        "exchange": exchange,
        "symbol": symbol,
        "predicted_rate": predicted_rate,
        "method": "api",
        "confidence": 0.9,
    })

    # Add payment
    validator.payments.append({
        "timestamp": base_time - 1800000,
        "datetime": datetime.fromtimestamp((base_time - 1800000) / 1000, UTC),
        "exchange": exchange,
        "symbol": symbol,
        "actual_rate": actual_rate,
        "payment_amount": 1.0,
        "position_size": 10000.0,
    })

    # Act
    metrics = validator.calculate_metrics(exchange, symbol)

    # Assert
    assert metrics["bias"] is not None
    assert abs(metrics["bias"] - expected_error) < 1e-10
    assert metrics["mae"] is not None
    assert abs(metrics["mae"] - abs(expected_error)) < 1e-10
    assert metrics["rmse"] is not None
    assert abs(metrics["rmse"] - abs(expected_error)) < 1e-10


@pytest.mark.parametrize(
    ("method", "confidence"),
    [
        ("api", 1.0),
        ("model", 0.8),
        ("hybrid", 0.95),
        ("manual", 0.5),
    ],
)
def test_record_prediction_methods_parametrized(
    validator: FundingRateValidator,
    method: str,
    confidence: float,
) -> None:
    """Test recording predictions with different methods and confidence levels."""
    # Act
    validator.record_prediction("hyperliquid", "BTC-PERP", 0.0001, method, confidence)

    # Assert
    assert len(validator.predictions) == 1
    prediction = validator.predictions[0]
    assert prediction["method"] == method
    assert prediction["confidence"] == confidence
