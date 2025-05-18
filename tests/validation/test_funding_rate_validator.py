"""
Tests for the FundingRateValidator class.
"""

import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from cyberdelta.validation.funding_rate_validator import FundingRateValidator


class TestFundingRateValidator:
    """Test suite for the FundingRateValidator class."""

    @pytest.fixture
    def validator(self) -> FundingRateValidator:
        """Create a validator instance for testing."""
        config = MagicMock()
        return FundingRateValidator(config)

    def test_record_prediction(self, validator: FundingRateValidator) -> None:
        """Test recording a funding rate prediction."""
        # Record a prediction
        validator.record_prediction("hyperliquid", "BTC", 0.0001, "api", 0.9)

        # Verify it was stored
        assert len(validator.predictions) == 1
        prediction = validator.predictions[0]

        assert prediction["exchange"] == "hyperliquid"
        assert prediction["symbol"] == "BTC"
        assert prediction["predicted_rate"] == 0.0001
        assert prediction["method"] == "api"
        assert prediction["confidence"] == 0.9
        assert "timestamp" in prediction
        assert isinstance(prediction["datetime"], datetime)

    def test_record_payment(self, validator: FundingRateValidator) -> None:
        """Test recording an actual funding payment."""
        # Record a payment
        validator.record_payment("hyperliquid", "BTC", 0.0001, 0.5, 10.0)

        # Verify it was stored
        assert len(validator.payments) == 1
        payment = validator.payments[0]

        assert payment["exchange"] == "hyperliquid"
        assert payment["symbol"] == "BTC"
        assert payment["actual_rate"] == 0.0001
        assert payment["payment_amount"] == 0.5
        assert payment["position_size"] == 10.0
        assert "timestamp" in payment
        assert isinstance(payment["datetime"], datetime)

    def test_calculate_metrics_no_data(self, validator: FundingRateValidator) -> None:
        """Test calculating metrics with no data."""
        metrics = validator.calculate_metrics("hyperliquid", "BTC")

        assert metrics["rmse"] is None
        assert metrics["mae"] is None
        assert metrics["bias"] is None
        assert metrics["prediction_count"] == 0
        assert metrics["payment_count"] == 0

    def test_calculate_metrics_with_data(self, validator: FundingRateValidator) -> None:
        """Test calculating metrics with sample data."""
        # Add test data
        # First prediction
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        time.sleep(0.01)  # Ensure different timestamps

        # First payment (after first prediction)
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)
        time.sleep(0.01)

        # Second prediction
        validator.record_prediction("hyperliquid", "BTC", 0.0008, "api", 0.9)
        time.sleep(0.01)

        # Second payment (after second prediction)
        validator.record_payment("hyperliquid", "BTC", 0.0009, 0.9, 100.0)

        # Calculate metrics
        metrics = validator.calculate_metrics("hyperliquid", "BTC")

        # Verify metrics
        assert metrics["rmse"] is not None
        assert metrics["mae"] is not None
        assert metrics["bias"] is not None
        assert metrics["prediction_count"] == 2
        assert metrics["payment_count"] == 2
        assert metrics["matched_count"] == 2

        # Expected metrics:
        # First pair: error = 0.0010 - 0.0012 = -0.0002
        # Second pair: error = 0.0008 - 0.0009 = -0.0001
        # RMSE = sqrt(mean([-0.0002, -0.0001]^2))
        #      = sqrt(mean([0.00000004, 0.00000001]))
        #      = sqrt(0.000000025) = 0.00015811
        # MAE = mean(abs([-0.0002, -0.0001])) = mean([0.0002, 0.0001]) = 0.00015
        # Bias = mean([-0.0002, -0.0001]) = -0.00015

        # Allow for small floating-point differences
        assert abs(metrics["rmse"] - 0.00015811) < 0.0001
        assert abs(metrics["mae"] - 0.00015) < 0.0001
        assert abs(metrics["bias"] - (-0.00015)) < 0.0001

    def test_get_validation_report(self, validator: FundingRateValidator) -> None:
        """Test generating a validation report."""
        # Add data for two exchange-symbol pairs
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)

        validator.record_prediction("backpack", "ETH", 0.0005, "api", 0.9)
        validator.record_payment("backpack", "ETH", 0.0004, 0.4, 50.0)

        # Generate report
        report = validator.get_validation_report()

        # Verify report structure
        assert "hyperliquid" in report
        assert "backpack" in report
        assert "BTC" in report["hyperliquid"]
        assert "ETH" in report["backpack"]

        # Verify report contents
        assert report["hyperliquid"]["BTC"]["prediction_count"] == 1
        assert report["hyperliquid"]["BTC"]["payment_count"] == 1
        assert report["backpack"]["ETH"]["prediction_count"] == 1
        assert report["backpack"]["ETH"]["payment_count"] == 1

    def test_get_recent_predictions(self, validator: FundingRateValidator) -> None:
        """Test retrieving recent predictions."""
        # Add predictions
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        validator.record_prediction("hyperliquid", "ETH", 0.0005, "api", 0.8)
        validator.record_prediction("backpack", "BTC", 0.0008, "model", 0.7)

        # Get all predictions
        predictions = validator.get_recent_predictions()
        assert len(predictions) == 3

        # Get predictions for a specific exchange
        predictions = validator.get_recent_predictions(exchange="hyperliquid")
        assert len(predictions) == 2
        assert all(p["exchange"] == "hyperliquid" for p in predictions)

        # Get predictions for a specific symbol
        predictions = validator.get_recent_predictions(symbol="BTC")
        assert len(predictions) == 2
        assert all(p["symbol"] == "BTC" for p in predictions)

        # Get predictions for a specific exchange and symbol
        predictions = validator.get_recent_predictions(exchange="hyperliquid", symbol="BTC")
        assert len(predictions) == 1
        assert predictions[0]["exchange"] == "hyperliquid"
        assert predictions[0]["symbol"] == "BTC"

    def test_get_recent_payments(self, validator: FundingRateValidator) -> None:
        """Test retrieving recent payments."""
        # Add payments
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)
        validator.record_payment("hyperliquid", "ETH", 0.0006, 0.6, 80.0)
        validator.record_payment("backpack", "BTC", 0.0009, 0.9, 90.0)

        # Get all payments
        payments = validator.get_recent_payments()
        assert len(payments) == 3

        # Get payments for a specific exchange
        payments = validator.get_recent_payments(exchange="hyperliquid")
        assert len(payments) == 2
        assert all(p["exchange"] == "hyperliquid" for p in payments)

        # Get payments for a specific symbol
        payments = validator.get_recent_payments(symbol="BTC")
        assert len(payments) == 2
        assert all(p["symbol"] == "BTC" for p in payments)

        # Get payments for a specific exchange and symbol
        payments = validator.get_recent_payments(exchange="hyperliquid", symbol="BTC")
        assert len(payments) == 1
        assert payments[0]["exchange"] == "hyperliquid"
        assert payments[0]["symbol"] == "BTC"

    def test_get_prediction_history(self, validator: FundingRateValidator) -> None:
        """Test retrieving prediction history."""
        # Add data
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        time.sleep(0.01)
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)
        time.sleep(0.01)
        validator.record_prediction("hyperliquid", "BTC", 0.0008, "api", 0.9)
        time.sleep(0.01)
        validator.record_payment("hyperliquid", "BTC", 0.0009, 0.9, 100.0)

        # Get history
        history = validator.get_prediction_history("hyperliquid", "BTC")

        # Verify structure
        assert "predictions" in history
        assert "actuals" in history
        assert "timestamps" in history["predictions"]
        assert "rates" in history["predictions"]
        assert "timestamps" in history["actuals"]
        assert "rates" in history["actuals"]

        # Verify data
        assert len(history["predictions"]["timestamps"]) == 2
        assert len(history["predictions"]["rates"]) == 2
        assert len(history["actuals"]["timestamps"]) == 2
        assert len(history["actuals"]["rates"]) == 2

        # Verify values
        assert history["predictions"]["rates"] == [0.0010, 0.0008]
        assert history["actuals"]["rates"] == [0.0012, 0.0009]

    def test_clear_old_data(self, validator: FundingRateValidator) -> None:
        """Test clearing old data."""
        # Create some data
        # Current data
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)

        # Manually add old data (100 days ago)
        old_time = datetime.now() - timedelta(days=100)
        old_timestamp = int(old_time.timestamp() * 1000)

        validator.predictions.append({
            "timestamp": old_timestamp,
            "datetime": old_time,
            "exchange": "hyperliquid",
            "symbol": "ETH",
            "predicted_rate": 0.0005,
            "method": "api",
            "confidence": 0.8,
        })

        validator.payments.append({
            "timestamp": old_timestamp,
            "datetime": old_time,
            "exchange": "hyperliquid",
            "symbol": "ETH",
            "actual_rate": 0.0006,
            "payment_amount": 0.6,
            "position_size": 80.0,
        })

        # Verify we have 4 total records
        assert len(validator.predictions) == 2
        assert len(validator.payments) == 2

        # Clear old data (keep only last 30 days)
        validator.clear_old_data(days_to_keep=30)

        # Verify old data is removed
        assert len(validator.predictions) == 1
        assert len(validator.payments) == 1

        # Verify remaining data is the current data
        assert validator.predictions[0]["symbol"] == "BTC"
        assert validator.payments[0]["symbol"] == "BTC"
